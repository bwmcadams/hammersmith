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

package com.mongodb.async

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.net.InetSocketAddress

import org.bson.util.Logging
import org.bson.collection._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.nio.ByteOrder
import com.mongodb.async.wire._
import scala.collection.JavaConversions._
import com.mongodb.async.futures._
import org.jboss.netty.buffer._
import org.bson._
import com.twitter.conversions.time._
import com.twitter.util.{ JavaTimer }

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue , ConcurrentHashMap , Executors}
import scala.collection.mutable.{ConcurrentMap , WeakHashMap}
import com.mongodb.async.util.{ConcurrentQueue , CursorCleaningTimer}

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

  log.info("Initializing MongoConnectionHandler.")
  MongoConnection.cleaningTimer.acquire(this)

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
      runCommand("admin", Document("isMaster" -> 1))(SimpleRequestFutures.command((doc: Document) => {
        log.info("Got a result from command: %s", doc)
        isMaster = doc.getAsOrElse[Boolean]("ismaster", false)
        maxBSONObjectSize = doc.getAsOrElse[Int]("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize)
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
  protected[mongodb] def runCommand[A <% BSONDocument](ns: String, cmd: A)(f: SingleDocQueryRequestFuture) {
    log.trace("Attempting to run command '%s' on DB '%s.$cmd', against RequestFuture: '%s'", cmd, ns, f)
    val qMsg = QueryMessage(ns + ".$cmd", 0, -1, cmd)
    log.trace("Created Query Message: %s, id: %d", qMsg, qMsg.requestID)
    send(qMsg, f)
  }

  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture) = MongoConnection.send(msg, f)

  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def apply(dbName: String): DB = database(dbName)

  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def database(dbName: String): DB = new DB(dbName)(this)

  def databaseNames(callback: Seq[String] => Unit) {
    runCommand("admin", Document("listDatabases" -> 1))(SimpleRequestFutures.command((doc: Document) => {
      log.debug("Got a result from 'listDatabases' command: %s", doc)
      if (!doc.isEmpty) {
        val dbs = {
          val lst = doc.as[BSONList]("databases").asList
          lst.map(_.asInstanceOf[Document].as[String]("name"))
        }
        callback(dbs)
      } else {
        log.warning("Command 'listDatabases' failed. Doc: %s", doc)
        callback(List.empty[String])
      }
    }))
  }

  val handler: MongoConnectionHandler

  def connected_? = _connected.get

  val addr: InetSocketAddress

  protected[mongodb] var _writeConcern: WriteConcern = WriteConcern.Normal


  protected[mongodb] def shutdown() {
    log.debug("Shutting Down & Cleaning up connection handler.")
    MongoConnection.cleaningTimer.stop(this)
  }
  /**
   *
   * Set the write concern for this database.
   * Will be used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * @param concern (WriteConcern) The write concern to use
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/mongodb-single-server-data-durability-guide/
   */
  def writeConcern_=(concern: WriteConcern) = _writeConcern = concern

  /**
   *
   * get the write concern for this database,
   * which is used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/mongodb-single-server-data-durability-guide/
   */
  def writeConcern = _writeConcern
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

  protected[mongodb] val dispatcher: ConcurrentMap[Int, CompletableRequest] =
    new ConcurrentHashMap[Int, CompletableRequest]()

  protected[mongodb] val cleaningTimer = new CursorCleaningTimer()

  /**
   * Cursors that need to be cleaned up
   * Weak is GOOD.  The idea here with a WeakHashMap is that internally
   * the Key is stored as a WeakReference.
   *
   * This means that a channel being in the deadCursors map will NOT PREVENT IT
   * from being garbage collected.
   */
  protected[mongodb] val deadCursors = new WeakHashMap[Channel, ConcurrentQueue[Long]]

  def apply(hostname: String = "localhost", port: Int = 27017) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    new DirectConnection(new InetSocketAddress(hostname, port))
  }

  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture)(implicit channel: Channel, maxBSONObjectSize: Int) {
    // TODO - Better pre-estimation of buffer size - We don't have a length attributed to the Message yet
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN,
      if (maxBSONObjectSize > 0) maxBSONObjectSize else 1024 * 1024 * 4))
    log.trace("Put msg id: %s f: %s into dispatcher: %s", msg.requestID, f, dispatcher)
    dispatcher.put(msg.requestID, CompletableRequest(msg, f))
    log.trace("PreWrite with outStream '%s'", outStream)
    msg.write(outStream)
    log.debug("Writing Message '%s' out to Channel via stream '%s'.", msg, outStream)
    channel.write(outStream.buffer())
  }


  /**
   * Deferred - doesn't actually happen immediately
   */
  protected[mongodb] def killCursors(ids: Long*)(implicit channel: Channel) {
    log.debug("Adding Dead Cursors to cleanup list: %s on Channel: %s", ids, channel)
    deadCursors.getOrElseUpdate(channel, new ConcurrentQueue[Long]) ++= ids
  }

  /**
   *  Clean up any resources (Typically cursors)
   *  Called regularly by a managed CursorCleaner thread.
   */
  protected[mongodb] def cleanup() {
    log.trace("Cursor Cleanup running.")
    if (deadCursors.isEmpty) {
      log.debug("No Dead Cursors.")
    } else {
      deadCursors.foreach((entry: (Channel, ConcurrentQueue[Long])) => {
        // TODO - ensure no concurrency issues / blockages here.
        log.debug("Pre DeQueue: %s", entry._2.length)
        val msg = KillCursorsMessage(entry._2.dequeueAll())
        log.debug("Post DeQueue: %s", entry._2.length)
        log.trace("Killing Cursors with Message: %s with %d cursors.", msg, msg.numCursors)
        MongoConnection.send(msg, NoOpRequestFuture)(entry._1, 1024 * 1024 * 4) // todo  flexible BSON although I doubt we'll need a 16 meg killcursors
      })
    }
  }
}

