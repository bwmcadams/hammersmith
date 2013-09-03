/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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
package hammersmith

import java.net.InetSocketAddress
import akka.pattern.{ ask, pipe }
import akka.actor._

import akka.io.{IO => IOFaçade, _}
import akka.event.Logging
import akka.io.Tcp.Connected
import akka.io.Tcp.Received
import akka.io.Tcp.Connect
import akka.io.Tcp.CommandFailed
import hammersmith.wire._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import hammersmith.bson.{ImmutableBSONDocumentParser, SerializableBSONObject, BSONParser}
import hammersmith.collection.immutable.{Document => ImmutableDocument}
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.io.Tcp.CommandFailed
import scala.Some
import akka.io.Tcp.ErrorClosed
import akka.io.Tcp.Received
import hammersmith.collection.Implicits.SerializableImmutableDocument
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.io.Tcp.CommandFailed
import scala.Some
import hammersmith.PendingOp
import akka.io.Tcp.ErrorClosed
import akka.io.Tcp.Received
import akka.util.ByteString
import hammersmith.io.MongoFrameHandler

/**
 *
 * Direct Connection to a single mongod (or mongos) instance
 *
 */
// TODO - Should there be one handler instance per connection? Or a pool of handlers/single handler shared? (One message at a time as actor...)
case class InitializeConnection(addr: InetSocketAddress)

case class PendingOp(sender: ActorRef, request: MongoRequest)

/**
 * The actor that actually proxies connection
 */
class DirectMongoDBConnector(val serverAddress: InetSocketAddress) extends Actor
  with ActorLogging
  with Stash {

  import Tcp._
  import context.system

  val tcpManager = IOFaçade(Tcp)
  /**
   * map of ops to be completed.
   * todo - some kind of cleanup mechanism , maybe involving weakMap, for futures that have timed out.
   */
  private val dispatcher = scala.collection.mutable.HashMap.empty[Int /** requestID */, PendingOp]
  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  /* this is an odd one. it seems iteratees from the old IO package ARENT deprecated
   * but they haven't been ported forward/documented in the NEW IO. I see no way in the NEW
   * IO to get that behavior however...
   * I somewhat shamelessly stole some of the model of Iteratee use from Raiku's code ( as well
   * as the Iteratee code from a previous pass at moving Hammersmith to AkkaIO ) */
  protected val state = IO.IterateeRef.async

  private var connection: Option[ActorRef] = None

  log.debug(s"Starting up a Direct Connection Actor to connect to '$serverAddress'")
  // todo - inherit keepalive setting from connection request
  tcpManager ! Connect(serverAddress, options = List(SO.KeepAlive(true), SO.TcpNoDelay(true)))

  override def postStop = {
    tcpManager ! Close

    // reset the Iteratee state
    state(IO.EOF)
  }

  def receive = {
    case CommandFailed(_: Connect) ⇒
      // TODO - notify the connected client
      log.error("Network Connection Failed")
      context stop self

    case c @ Connected(remote, local) =>
      /**
       * I believe ultimately we should register a separate actor for all reads;
       * If we use ourself as the listener we'll end up with a massive bandwidth limit as
       * the mailbox dequeues one at a time
       * However, then dispatching responses to the original requester becomes hard
       * w/o sharing state.
       * This also effectively limits some of our rate on the individual connection.
       * While the API might let it APPEAR we are pushing lots of writes while
       * doing lots of reads off the same connection, that's a false view from NIO...
       * it all buffers under the covers.
       */
      sender ! Register(self)
      log.debug(s"Established a direct connection to MongoDB at '$serverAddress'")
      connection = Some(sender)
      // TODO - SSL support, if we can get a hold of a copy of the SSL build (I think the source is free, bins aren't)
      val init = TcpPipelineHandler.withLogger(log,
          new MongoFrameHandler >>
          new TcpReadWriteAdapter >>
          new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000))


      val pipeline = context.actorOf(TcpPipelineHandler.props(init, sender, self).withDeploy(Deploy.local) /* ensure it can't be remoted */)

      context watch self

      sender ! Tcp.Register(pipeline)
      context become connectedBehavior
      // dequeue any stashed messages
      unstashAll()
  }

  def connectedBehavior: Actor.Receive = {
    case CommandFailed(w: Write) ⇒ // O/S buffer was full
    case Received(data) =>
      log.debug("Received Data {}", data)
      // TODO - Dispatch!
    // a non-mutating operation
    case r: MongoRequest => connection match {
      case None =>
        // stash the message for later
        stash()
      case Some(conn) =>
        conn ! r
    }
    // a mutating operation
    // todo - mutating operations need to handle a GetLastError setup
    case w: MongoMutationRequest => connection match {
      case None =>
        // stash the message for later
        stash()
      case Some(conn) =>
        conn ! w
    }

    case CommandFailed(cmd) =>
      // TODO - We need to handle backpressure/buffering of NIO ... which Akka expects us to do
      // this is related to that, I believe.
      log.error("Network Command Failed: " + cmd.failureMessage)
    case ConfirmedClosed =>
      log.debug(s"Confirmed closing of connection to '$serverAddress'")
    case Aborted =>
      log.error(s"Aborted connection to '$serverAddress'")
      // todo - more discrete exceptions
      throw new MongoException(s"Connection to '$serverAddress' aborted.")
    case PeerClosed =>
      log.error(s"Peer connection to '$serverAddress' disconnected unexpectedly.")
      // todo - more discrete exceptions
      throw new MongoException(s"Peer connection to '$serverAddress' disconnected unexpectedly.")
    case ErrorClosed(cause) =>
      log.error(s"Error in connection to '$serverAddress'; connection closed. Cause: '$cause'")
      // todo - more discrete exceptions
      throw new MongoException(s"Error in connection to '$serverAddress'; connection closed. Cause: '$cause'")
  }

  def send(cmd: Tcp.Command) = connection match {
    case Some(conn) =>
      conn ! cmd
    case None =>
      throw new IllegalStateException("No connection actor registered; cannot send.")
  }
}


// TODO - Move me into another package
trait MongoRequest {
  type DocType
  def msg: MongoClientMessage
  def requestID = msg.requestID
  def decoder: SerializableBSONObject[DocType]
}

// todo - how do we instantiate/manage the cursor?
case class QueryRequest[T : SerializableBSONObject](val msg: QueryMessage) extends MongoRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * A cursor "getMore" - next batch fetch.
 * @tparam T Document type to return
 */
case class GetMoreRequest[T : SerializableBSONObject](val msg: GetMoreMessage) extends MongoRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * A request for a single document, where user has said to skip the cursor.
 * TODO: Bake a specific "findOne" client message so that this guarantees type safety on the call
 * @tparam T Document type to return
 */
case class FindOneRequest[T : SerializableBSONObject](val msg: QueryMessage) extends MongoRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * FindAndModify Command, only ever returns one doc.
 */
case class FindAndModifyRequest[T : SerializableBSONObject](val msg: QueryMessage) extends MongoRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}


/**
 * Anything that mutates data on the server,
 * specifically where a WriteConcern is necessary.
 *
 * This doesn't include commands such as findAndModify as they
 * don't have a sane place in the protocol and we can't WriteConcern them.
 */
trait MongoMutationRequest extends MongoRequest {
  override def msg: MongoClientWriteMessage
  def writeConcern: WriteConcern
}

/**
 * Insert a *single* document.
 */
case class InsertRequest[T : SerializableBSONObject](val msg: SingleInsertMessage, val writeConcern: WriteConcern) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Insert multiple documents.
 *
 * Keep in mind, that WriteConcern behavior may be wonky if you do a batchInsert
 * I believe the behavior of MongoDB will cause getLastError to indicate the LAST error
 * on your batch ---- not the first, or all of them.
 */
case class BatchInsertRequest[T : SerializableBSONObject](val msg: BatchInsertMessage, val writeConcern: WriteConcern) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Update a *single* document.
 */
case class UpdateRequest[T : SerializableBSONObject](val msg: SingleUpdateMessage, val writeConcern: WriteConcern) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Update multiple documents.
 *
 */
case class BatchUpdateRequest[T : SerializableBSONObject](val msg: BatchUpdateMessage, val writeConcern: WriteConcern) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Delete (multiple, by default) documents.
 * There is no response to a MongoDB Delete, so hardcoded response to Immutable
 */
case class DeleteRequest(val msg: DeleteMessage, val writeConcern: WriteConcern) extends MongoMutationRequest {
  type DocType = ImmutableDocument
  val decoder = SerializableImmutableDocument
}
