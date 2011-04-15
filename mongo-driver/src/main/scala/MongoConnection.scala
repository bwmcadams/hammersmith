/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.net.InetSocketAddress

import org.bson.util.Logging
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.nio.ByteOrder
import scala.collection.Map
import com.mongodb.wire._
import java.util.concurrent.{ ConcurrentHashMap, Executors }
import scala.collection.JavaConversions._
import com.mongodb.futures._
import org.jboss.netty.buffer._
import scala.collection.mutable.ConcurrentMap
import org.bson._
import java.util.concurrent.atomic.AtomicBoolean

/**
* Base trait for all connections, be it direct, replica set, etc
*
* This contains common code for any type of connection.
*
* NOTE: Connection instances are instances of a *POOL*, always.
*
* @author Brendan W. McAdams <brendan@10gen.com>
* @since 0.1
*/
abstract class MongoConnection extends Logging {

  // TODO - MAKE THESE IMMUTABLE AND/OR PASS TO PLACES THAT NEED TO ALLOCATE BUFFERS
  /** Maximum size of BSON this server allows. */
  protected implicit var maxBSONObjectSize = 0
  protected var isMaster = false

  protected val _connected = new AtomicBoolean(false)


  /* TODO - Can we reuse these factories across multiple connections??? */

  /**
   * Factory for client socket channels, reused by all connectors where possible.
   */
  val channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool,
                                                         Executors.newCachedThreadPool)

  protected implicit val bootstrap = new ClientBootstrap(channelFactory)

  bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    def getPipeline = Channels.pipeline(new BSONFrameDecoder(), handler)
  })

  bootstrap.setOption("remoteAddress", addr)

  private val _f = bootstrap.connect()
  protected implicit val channel = _f.awaitUninterruptibly.getChannel

  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("keepAlive", true)
  if (!_f.isSuccess) {
    log.error("Failed to connect.", _f.getCause)
    bootstrap.releaseExternalResources()
  } else {
    log.debug("Connected and retrieved a write channel (%s)", channel)
    checkMaster()
  }


  /**
   * Utility method to pull back a number of pieces of information
   * including maxBSONObjectSize, and sort of serves as a verification
   * of a live connection.
   *
   * @param force Forces the isMaster call to run regardless of cached status
   * @param requireMaster Requires a master to be found or throws an Exception
   * @throws MongoException
   */
  def checkMaster(force: Boolean = false, requireMaster: Boolean = true) {
    if (maxBSONObjectSize == 0 || force) {
      log.debug("Checking Master Status... (BSON Size: %d Force? %s)", maxBSONObjectSize, force)
      val gotIsMaster = new AtomicBoolean(false)
      runCommand("admin", Document("isMaster" -> 1), RequestFutures.command((doc: Option[Document], res: FutureResult) => {
        log.info("Got a result from command: %s", doc)
        doc foreach { x =>
          isMaster = x.getOrElse("ismaster", false).asInstanceOf[Boolean]
          maxBSONObjectSize = x.getOrElse("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize).asInstanceOf[Int]
        }
        gotIsMaster.set(true)
        if (requireMaster && !isMaster) throw new Exception("Couldn't find a master.") else _connected.set(true)
        handler.maxBSONObjectSize = maxBSONObjectSize
        log.debug("Server Status read.  Is Master? %s MaxBSONSize: %s", isMaster, maxBSONObjectSize)
      }))
    } else {
      log.debug("Already have cached master status. Skipping.")
    }
  }


  /**
   * WARNING: You *must* use an ordered list or commands won't work
   */
  protected[mongodb] def runCommand[A <% BSONDocument](ns: String, cmd: A, f: SingleDocQueryRequestFuture) {
    log.trace("Attempting to run command '%s' on DB '%s.$cmd', against RequestFuture: '%s'", cmd, ns, f)
    val qMsg = QueryMessage(ns + ".$cmd", 0, -1, cmd)
    log.trace("Created Query Message: %s, id: %d", qMsg, qMsg.requestID)
    send(qMsg, f)
  }

  protected[mongodb] def send(msg: MongoMessage, f: RequestFuture) = MongoConnection.send(msg, f)


  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def apply(dbName: String): DB = database(dbName)

  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def database(dbName: String): DB = new DB(dbName)(this)

  def databaseNames(callback: Seq[String] => Unit) {
    runCommand("admin", Document("listDatabases" -> 1), RequestFutures.command((doc: Option[Document], res: FutureResult) => {
      log.debug("Got a result from 'listDatabases' command: %s", doc)
      if (res.ok && doc.isDefined) {
        val dbs = doc.get.as[BSONList]("databases").asList.map(_.asInstanceOf[Document].as[String]("name"))
        callback(dbs)
      } else {
        log.warning("Command 'listDatabases' failed: %s / Doc: %s", res, doc.getOrElse(Document.empty))
        callback(List.empty[String])
      }
    }))
  }


  val handler: MongoConnectionHandler

  def connected_? = _connected.get

  val addr: InetSocketAddress

}

trait MongoConnectionHandler extends SimpleChannelHandler with Logging {
  val bootstrap: ClientBootstrap

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val buf = e.getMessage.asInstanceOf[ChannelBuffer]
    log.debug("Incoming Message received on (%s) Length: %s", buf, buf.readableBytes())
    MongoMessage.unapply(new ChannelBufferInputStream(buf)) match {
      case reply: ReplyMessage => {
        log.trace("Reply Message Received: %s", reply)
        // Dispatch the reply, separate into requisite parts, etc
        /**
        * Note - it is entirely OK to stuff a single result into
        * a Cursor, but not multiple results into a single.  Should be obvious.
        */
        MongoConnection.dispatcher.get(reply.header.responseTo) match {
          case Some(singleResult: SingleDocQueryRequestFuture) => {
            log.trace("Single Document Request Future.")
            // This may actually be better as a disableable assert but for now i want it hard.
            require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a SingleDocQueryRequestFuture.")
            // Check error state
            // TODO - Different handling depending on type of op, GetLastError etc
            // Though - GetLastError could dispatch back out again here and not invoke the callback!
            if (reply.cursorNotFound) {
              log.trace("Cursor Not Found.")
              singleResult.result = FutureResult(false, Some("Cursor Not Found"), 0)
            } else if (reply.queryFailure) {
              log.trace("Query Failure")
              // Attempt to grab the $err document
              val err = reply.documents.headOption match {
                case Some(errDoc) => {
                  log.trace("Error Document found: %s", errDoc)
                  errDoc.getOrElse("$err", "Unknown Error.").toString
                }
                case None => {
                  log.warn("No Error Document Found.")
                  "Unknown Error."
                }
              }
              singleResult.result = FutureResult(false, Some(err), 0)
            } else {
              singleResult.result = FutureResult(true, None, reply.numReturned)
              singleResult.element = reply.documents.head.asInstanceOf[singleResult.T]
            }
            singleResult()
          }
          case Some(cursorResult: CursorQueryRequestFuture) => {
            log.debug("Cursor Result Wanted: %s", reply.documents)
            log.trace("Cursor Document Request Future.")
            // TODO - Different handling depending on type of op, GetLastError etc
            // Though - GetLastError could dispatch back out again here and not invoke the callback!
            if (reply.cursorNotFound) {
              log.debug("Cursor Not Found.")
              cursorResult.result = FutureResult(false, Some("Cursor Not Found"), 0)
            } else if (reply.queryFailure) {
              log.debug("Query Failure")
              // Attempt to grab the $err document
              val err = reply.documents.headOption match {
                case Some(errDoc) => {
                  log.trace("Error Document found: %s", errDoc)
                  errDoc.getOrElse("$err", "Unknown Error.").toString
                }
                case None => {
                  log.warn("No Error Document Found.")
                  "Unknown Error."
                }
              }
              cursorResult.result = FutureResult(false, Some(err), 0)
            } else {
              cursorResult.result = FutureResult(true, None, reply.numReturned)
              cursorResult.element = new Cursor(reply)(ctx).asInstanceOf[cursorResult.T]
            }
            cursorResult()
          }
          case None => {
            /**
            * Even when no response is wanted a 'Default' callback should be regged so
            * this is definitely warnable, for now.
            */
            log.warn("No registered callback for request ID '%d'.  This may or may not be a bug.",
                     reply.header.responseTo)
          }
        }
      }
      case default => {
        log.warn("Unknown message type '%s'; ignoring.", default)
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    // TODO - pass this up to the user layer?
    log.error(e.getCause, "Uncaught exception Caught in ConnectionHandler")
    // TODO - Close connection?
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.warn("Disconnected from '%s'", remoteAddress)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Channel Closed to '%s'", remoteAddress)
    // TODO - Reconnect?
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Connected to '%s' (Configging Channel to Little Endian)", remoteAddress)
    e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))
  }

  def remoteAddress = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]

  var maxBSONObjectSize = 1024 * 4 * 4 // default
}

/**
 * Factory object for creating connections
 * based on things like URI Spec
 *
 * NOTE: Connection instances are instances of a *POOL*, always.
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @since 0.1
 */
object MongoConnection extends Logging {

  protected[mongodb] val dispatcher: ConcurrentMap[Int, RequestFuture] =
    new ConcurrentHashMap[Int, RequestFuture]()

  def apply(hostname: String = "localhost", port: Int = 27017) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    new DirectConnection(new InetSocketAddress(hostname, port))
  }

  protected[mongodb] def send(msg: MongoMessage, f: RequestFuture)
                             (implicit channel: Channel, maxBSONObjectSize: Int) {
    // TODO - Better pre-estimation of buffer size - We don't have a length attributed to the Message yet
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN,
                                                                               if (maxBSONObjectSize > 0) maxBSONObjectSize else 1024 * 1024 * 4
                                                                              ))
    log.trace("Put msg id: %s f: %s into dispatcher: %s", msg.requestID, f, dispatcher)
    dispatcher.put(msg.requestID, f)
    log.trace("PreWrite with outStream '%s'", outStream)
    msg.write(outStream)
    log.trace("Writing Message '%s' out to Channel via stream '%s'.", msg, outStream)
    channel.write(outStream.buffer())
  }
}

