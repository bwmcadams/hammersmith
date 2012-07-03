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

import java.net.InetSocketAddress
import org.bson._
import org.bson.collection._
import java.nio.{ByteBuffer, ByteOrder}
import com.mongodb.async.wire._
import scala.collection.JavaConversions._
import com.mongodb.async.futures._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, Executors, SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable.{ConcurrentMap, WeakHashMap}
import com.mongodb.async.util.{ConcurrentQueue, CursorCleaningTimer}
import org.bson.types.ObjectId
import org.bson.util.Logging
import com.mongodb.async.util._
import akka.actor._
import akka.util.{ ByteString, ByteStringBuilder }
import akka.actor.IO.SocketHandle
import com.mongodb.io.ByteBufferInputStream
import scalaj.collection.Imports._
import java.util.ArrayList

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
class AkkaConnection(hostname: String, port: Int)(implicit actorSystem: ActorSystem) extends MongoConnectionActor {

  protected[mongodb] val dispatcher: ConcurrentMap[Int, CompletableRequest] =
    new ConcurrentHashMap[Int, CompletableRequest]()
  log.info("Initializing Akka Connection.")

  val address: InetSocketAddress = new InetSocketAddress(hostname, port)

  val socket = IOManager(context.system) connect (address)

  //MongoConnection.cleaningTimer.acquire(this)

  // TODO - MAKE THESE IMMUTABLE AND/OR PASS TO PLACES THAT NEED TO ALLOCATE BUFFERS
  /** Maximum size of BSON this server allows. */
  protected implicit var maxBSONObjectSize = MongoMessage.DefaultMaxBSONObjectSize
  protected var isMaster = false

  protected var connected: Boolean = false

  def connected_? = connected

  def onConnection = {
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
    if (!connected || force) {
      log.info("Checking Master Status... (BSON Size: %d Force? %s)", maxBSONObjectSize, force)
      val gotIsMaster = new AtomicBoolean(false)
      val qMsg = MongoConnection.createCommand("admin", Document("isMaster" -> 1))
      send(qMsg, SimpleRequestFutures.command((doc: Document) ⇒ {
        log.debug("Got a result from command: %s", doc)
        isMaster = doc.getAsOrElse[Boolean]("ismaster", false)
        maxBSONObjectSize = doc.getAsOrElse[Int]("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize)
        gotIsMaster.set(true)
        if (requireMaster && !isMaster) throw new Exception("Couldn't find a master.") //else _connectedState(true, maxBSONObjectSize)
        //handler.maxBSONObjectSize = maxBSONObjectSize
        log.info("Server Status read.  Is Master? %s MaxBSONSize: %s", isMaster, maxBSONObjectSize)
      }), _overrideLiveCheck = true)
    } else {
      log.debug("Already have cached master status. Skipping.")
    }
  }

  def send(msg: MongoClientMessage, f: RequestFuture, _overrideLiveCheck: Boolean = false)(implicit maxBSONObjectSize: Int, concern: WriteConcern = WriteConcern.Normal) = {
    //require(connected, "Channel is Closed.")

    val isWrite = f.isInstanceOf[WriteRequestFuture]
    val b = new ByteStringBuilder
    val outStream = b.asOutputStream
    log.info("Put msg id: %s f: %s into dispatcher: %s", msg.requestID, f, dispatcher)
    log.info("PreWrite with outStream '%s'", outStream)
    /**
     * We only setup dispatchers if it is a Non-Write Request or a Write Request w/ a Write Concern that necessitates GLE
     * The GLE / Safe write stuff is setup later
     */
    if (!isWrite) dispatcher.put(msg.requestID, CompletableRequest(msg, f))
    msg.write(outStream)
    log.info("Writing Message '%s' out to Channel via stream '%s'.", msg, outStream)

    /**
     * Determine if we need to execute a GetLastError (E.G. w > 0),
     * or execute a non-GLEed immediate callback against write requests.
     */
    // Quick callback when needed to be invoked immediately after write
    val writeCB: () ⇒ Unit = if (isWrite) {
      msg match {
        case wMsg: MongoClientWriteMessage ⇒ if (concern.safe_?) {
          val gle = MongoConnection.createCommand(wMsg.namespace.split("\\.")(0), Document("getlasterror" -> 1))
          log.info("Created a GetLastError Message: %s", gle)
          /**
           * We only setup dispatchers if it is a Non-Write Request or a Write Request w/ a Write Concern that necessitates GLE
           * Note we dispatch the GetLastError's ID but with the write message !
           */
          dispatcher.put(gle.requestID, CompletableRequest(msg, f))
          gle.write(outStream)
          log.info("Wrote a getLastError to the tail end of the output buffer.")
          () ⇒ {}
        } else () ⇒ { wMsg.ids.foreach(x ⇒ f((x, WriteResult(true)).asInstanceOf[f.T])) }
        case unknown ⇒ {
          val e = new IllegalArgumentException("Invalid type of message passed; WriteRequestFutures expect a MongoClientWriteMessage underneath them. Got " + unknown)
          log.info(e, "Error in write.")
          () ⇒ { f(e) }
        }
      }
    } else () ⇒ {}
    // todo - clean this up to be more automatic like the writeCB is

    val exec = (_maxBSON: Int) ⇒ {
      outStream.close()
      val frame = b.result()
      socket.write(frame)
      /** If no write Concern and it's a write, kick the callback now.*/
      writeCB()
    }
    // If the channel is open, it still doesn't mean we have a valid Mongo Connection.
    /*if (!channelState(channel).get && !_overrideLiveCheck) {
      log.info("Channel is not currently considered 'live' for MongoDB... May still be connecting or recovering from a Replica Set failover. Queueing operation. (override? %s) ", _overrideLiveCheck)
      channelOpQueue.getOrElseUpdate(channel, new ConcurrentQueue) += exec
    } else exec(maxBSONObjectSize)*/
    exec(maxBSONObjectSize)

  }
}

trait MongoConnectionActor extends Actor with Logging {

  def onConnection()

  def socket: SocketHandle

  val state = IO.IterateeRef.Map.async[IO.Handle]()(context.dispatcher)

  def receive = {
    case IO.Connected(server, address) =>
      log.info("Now connected to MongoDB at '%s'", address)
      onConnection()

    case IO.Read(socket, bytes: ByteString) =>
      state(socket)(IO Chunk bytes)
      val source = sender
      log.info("Decoding bytestream")
      implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
      val len = bytes.iterator.getInt
      val frame = bytes.take(len)
      val msg = MongoMessage.unapply(new ByteBufferInputStream(List(frame.toByteBuffer).asJava))
      log.info("Mongo Message: " + msg)


    case IO.Closed(socket: IO.SocketHandle, cause) =>
      log.info("Socket has closed, cause: " + cause)
      state(socket)(IO EOF cause)
      throw(cause getOrElse new RuntimeException("Network Socket Closed: '%s'".format(cause)))

  }

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
object AkkaConnection extends Logging {

  val defaultActorSystem = ActorSystem.create("MongoDB-Hammersmith")


  protected[mongodb] val dispatcher: ConcurrentMap[Int, CompletableRequest] =
    new ConcurrentHashMap[Int, CompletableRequest]()

  def apply(hostname: String = "localhost", port: Int = 27017)(implicit actorSystem: ActorSystem = defaultActorSystem) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    actorSystem.actorOf(Props(new AkkaConnection(hostname, port)))
  }



}

