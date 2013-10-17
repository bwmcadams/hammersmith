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
import hammersmith.wire._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import hammersmith.bson.{BSONDocumentType, ImmutableBSONDocumentParser, SerializableBSONObject, BSONParser}
import hammersmith.collection.immutable.{Document => ImmutableDocument}
import hammersmith.collection.Implicits.SerializableImmutableDocument
import hammersmith.io.MongoFrameHandler
import akka.io.TcpPipelineHandler.{Init, WithinActorContext}
import akka.util.ByteString
import java.nio.ByteOrder
import hammersmith.wire.MessageHeader
import hammersmith.PendingOp
import hammersmith.collection.BSONDocument

/**
 *
 * Direct Connection to a single mongod (or mongos) instance
 *
 */
// TODO - Should there be one handler instance per connection? Or a pool of handlers/single handler shared? (One message at a time as actor...)

case class PendingOp(sender: ActorRef, request: MongoRequest)

/**
 * The actor that actually proxies connection
 */
class DirectMongoDBConnector(val serverAddress: InetSocketAddress, val requireMaster: Boolean = true) extends Actor
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


  log.debug(s"Starting up a Direct Connection Actor to connect to '$serverAddress'")
  // todo - inherit keepalive setting from connection request
  tcpManager ! Connect(serverAddress, options = List(SO.KeepAlive(true), SO.TcpNoDelay(true)))

  override def postStop = {
    tcpManager ! Close

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
      log.debug(s"Established a direct connection to MongoDB at '$serverAddress'")
      // TODO - SSL support, if we can get a hold of a copy of the SSL build (I think the source is free, bins aren't)
      val init = TcpPipelineHandler.withLogger(log,
          new MongoFrameHandler >>
          new TcpReadWriteAdapter >>
          new BackpressureBuffer(lowBytes = 100, highBytes = 1000, maxBytes = 1000000))


      // this is the actual connection context now
      val pipeline = context.actorOf(TcpPipelineHandler.props(init, sender, self).withDeploy(Deploy.local) /* ensure it can't be remoted */)

      context watch self

      sender ! Tcp.Register(pipeline)
      // invoke isMaster
      log.debug("Invoking MongoDB isMaster function")
      val isMaster = CommandRequest("admin", ImmutableDocument("isMaster" -> 1))
      log.debug("Created isMaster query '{}'", isMaster.msg)

      pipeline ! init.Command(isMaster.msg)
      context.become(awaitingReplyBehavior(init, pipeline, isMaster.requestID, { r: ReplyMessage =>
        val doc: ImmutableDocument = r.documents[ImmutableDocument].head
        log.debug("ismaster: '{}'", doc)
        if (requireMaster && !(doc.getAs[Boolean]("ismaster") == Some(true)))
          throw new IllegalStateException(s"Require Master; server @ '$serverAddress' is not master.")
        if (doc.getAs[Double]("maxBsonObjectSize").isEmpty)
          throw new IllegalStateException("Remote server does not report maxBsonObjectSize, probably an old server. Please use MongoDB 1.8+")
        // connected now, continue with proper setup & dequeue any stashed messages
      }))
  }

  def backpressureHighwaterBehavior(wire: Init[WithinActorContext, MongoMessage, MongoMessage], connection: ActorRef): Actor.Receive = {
    case BackpressureBuffer.LowWatermarkReached =>
      unstashAll()
      context.become(connectedBehavior(wire, connection))
    case _ =>
      // put message away until after high water mark is clear
      stash()
  }

  def awaitingReplyBehavior(wire: Init[WithinActorContext, MongoMessage, MongoMessage], connection: ActorRef, reqID: Int, onReply: ReplyMessage => Unit): Actor.Receive = {
    case wire.Event(r @ ReplyMessage(reqID)) =>
      onReply(r)
      context.become(connectedBehavior(wire, connection))
      unstashAll()
    case other =>
      stash()
  }

  def connectedBehavior(wire: Init[WithinActorContext, MongoMessage, MongoMessage], connection: ActorRef): Actor.Receive = {
    case BackpressureBuffer.HighWatermarkReached =>
      context.become(backpressureHighwaterBehavior(wire, connection))
    case CommandFailed(w: Write) ⇒ // O/S buffer was full
    // a mutating operation
    // todo - mutating operations need to handle a GetLastError setup
    case w: MongoMutationRequest =>
      log.debug("Writing MongoMutationRequest to connection '{}'", w)
      connection ! wire.Command(w.msg)
      // behave accordingly based on WriteConcern...
      if (w.writeConcern.blockingWrite_?) {
        // block out other requests until the write is completed
        context.become(awaitingReplyBehavior(wire, connection, w.requestID, { r: ReplyMessage =>
          // This is the getLastError result ... basically all writes get an Option[WriteResult]
          sender ! Some(WriteResult(r.documents.head))
        }))
      } else {
        // NOOP
        sender ! None
      }
    // a non-mutating operation
    case r: MongoRequest =>
      dispatcher += r.requestID -> PendingOp(sender, r)
      log.debug("Writing MongoRequest to connection '{}'", r)
      connection ! wire.Command(r.msg)
    case wire.Event(r: ReplyMessage) =>
      log.debug("ReplyMessage '{}' received on wire...", r)
      // try to match it up with dispatcher
      dispatcher.get(r.requestID) match {
        case None => log.error("Received a reply message w/ request ID {}, but no dispatcher entry.", r.requestID)
        case Some(PendingOp(sender, req)) =>
          // todo - match up based upon type of request
          log.info("Received a reply for request {}, documents decoded: {} \n *** TODO - FILL ME IN ***", r.requestID, r.documents(req.decoder))
      }
    case CommandFailed(cmd) =>
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

case class CommandRequest[T : SerializableBSONObject](val db: String, val doc: BSONDocument) extends MongoRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
  val msg = QueryMessage(db + ".$cmd", 0, -1, doc)
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
  def writeConcern = msg.writeConcern
}

/**
 * Insert a *single* document.
 */
case class InsertRequest[T : SerializableBSONObject](val msg: SingleInsertMessage) extends MongoMutationRequest {
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
case class BatchInsertRequest[T : SerializableBSONObject](val msg: BatchInsertMessage) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Update a *single* document.
 */
case class UpdateRequest[T : SerializableBSONObject](val msg: SingleUpdateMessage) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Update multiple documents.
 *
 */
case class BatchUpdateRequest[T : SerializableBSONObject](val msg: BatchUpdateMessage) extends MongoMutationRequest {
  type DocType = T
  val decoder = implicitly[SerializableBSONObject[T]]
}

/**
 * Delete (multiple, by default) documents.
 * There is no response to a MongoDB Delete, so hardcoded response to Immutable
 */
case class DeleteRequest(val msg: DeleteMessage) extends MongoMutationRequest {
  type DocType = ImmutableDocument
  val decoder = SerializableImmutableDocument
}
