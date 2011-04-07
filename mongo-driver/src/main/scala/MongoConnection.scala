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
import scala.collection.mutable.ConcurrentMap
import com.mongodb.futures._
import org.jboss.netty.buffer._
import org.bson.{Document , BSONDocument}

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

  /* TODO - Can we reuse these factories across multiple connections??? */
  /**
   * Factory for client socket channels, reused by all connectors where possible.
   */
  val channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool,
    Executors.newCachedThreadPool)

  implicit val bootstrap = new ClientBootstrap(channelFactory)

  bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    def getPipeline() = Channels.pipeline(new BSONFrameDecoder(), newHandler)
  })

  bootstrap.setOption("remoteAddress", addr)

  private val _f = bootstrap.connect()
  protected val channel = _f.awaitUninterruptibly.getChannel

  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("keepAlive", true)
  if (!_f.isSuccess) {
    log.error("Failed to connect.", _f.getCause)
    bootstrap.releaseExternalResources()
  } else {
    log.info("Connected and retrieved a write channel (%s)", channel)
    checkMaster()
  }

  protected[mongodb] val dispatcher = JConcurrentMapWrapper(new ConcurrentHashMap[Int, RequestFuture[_]])
  /**
   * Utility method to pull back a number of pieces of information
   * including maxBSONObjectSize, and sort of serves as a verification
   * of a live connection.
   *
   * @param force Forces the isMaster call to run regardless of cached status
   * @param requireMaster Requires a master to be found or throws an Exception
   * @throws MongoException
   */
  def checkMaster(force: Boolean = false, requireMaster: Boolean = true) = {
    if (maxBSONObjectSize == 0 || force) {
      log.debug("Checking Master Status... (BSON Size: %d Force? %s)", maxBSONObjectSize, force)
      readMaxBSONObjectSize()
    }

    log.debug("Already have cached master status. Skipping.")
    // NS = db + $CMD
  }

  def readMaxBSONObjectSize() = {
    runCommand("admin", Document("isMaster" -> 1), RequestFutures.command((doc: Option[Document], res: FutureResult) => {
      log.info("Got a result from command: %s", res)
    }))
  }

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   */
  protected[mongodb] def runCommand(ns: String, cmd: BSONDocument, f: CommandRequestFuture) {
    log.trace("Attempting to run command '%s' on DB '%s.$cmd', against CommandRequestFuture: '%s'", cmd, ns, f)
    val qMsg = QueryMessage(ns + ".$cmd", 0, -1, cmd)
    log.trace("Created Query Message: %s", qMsg)
    send(qMsg, f)
    // TODO Future for parsing
  }
  protected[mongodb] def send(msg: MongoMessage, f: RequestFuture[_]) {
    // TODO - Better pre-estimation of buffer size
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1024 * 1024 * 4))
    //    dispatcher.add((msg.requestID, f))
    log.trace("PreWrite with outStream '%s'", outStream)
    msg.write(outStream)
    log.trace("Writing Message '%s' out to Channel via stream '%s'.", msg, outStream)
    val handle = channel.write(outStream.buffer())
    // temporary for testing
    handle.awaitUninterruptibly()
  }

  def newHandler: DirectConnectionHandler

  val addr: InetSocketAddress

  // TODO - MAKE THESE IMMUTABLE
  /** Maximum size of BSON this serve allows. */
  var maxBSONObjectSize = 0

}

trait MongoConnectionHandler extends SimpleChannelHandler with Logging {
  val bootstrap: ClientBootstrap
  protected[mongodb] val dispatcher: ConcurrentMap[Int, RequestFuture[_]]

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val buf = e.getMessage.asInstanceOf[ChannelBuffer]
    log.debug("Incoming Message received on (%s) Length: %s", buf, buf.readableBytes())
    MongoMessage.unapply(new ChannelBufferInputStream(buf)) match {
      case reply: ReplyMessage => {
        log.trace("Reply Message Received: %s", reply)
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

  def apply(hostname: String, port: Int = 27017) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    new DirectConnection(new InetSocketAddress(hostname, port))
  }

}

