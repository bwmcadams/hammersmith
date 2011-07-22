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

import akka.actor._
import com.mongodb.async.wire._
import org.bson._
import org.bson.util.Logging
import org.bson.collection._

/**
 * A base class for actors that represent a mongod
 * connection. Right now this is just empty, all the
 * utility functions are in the companion object.
 *
 * Subclasses would include the single-channel DirectConnectionActor
 * and an actor derived from ActorPool.
 */
class ConnectionActor
  extends Logging {
  self => Actor

  import ConnectionActor._
}

object ConnectionActor
  extends Logging {
  // Messages we can handle
  sealed trait Incoming

  // client requests sent to us from app
  sealed trait SendClientMessage extends Incoming {
    val message: MongoClientMessage
    val overrideLiveCheck: Boolean // FIXME this means nothing now because we don't process messages on non-live actors
  }
  sealed trait SendClientWriteMessage extends SendClientMessage {
    val concern: WriteConcern
    override val message: MongoClientWriteMessage
  }
  case class SendClientCheckMasterMessage(force: Boolean, override val overrideLiveCheck: Boolean = false) extends SendClientMessage {
    override val message = ConnectionActor.createCommand("admin", Document("isMaster" -> 1))
  }
  case class SendClientGetMoreMessage(override val message: GetMoreMessage, override val overrideLiveCheck: Boolean = false) extends SendClientMessage
  case class SendClientCursorMessage(override val message: QueryMessage, override val overrideLiveCheck: Boolean = false) extends SendClientMessage
  case class SendClientSingleDocumentMessage(override val message: QueryMessage, override val overrideLiveCheck: Boolean = false) extends SendClientMessage
  case class SendClientOptionalSingleDocumentMessage(override val message: QueryMessage, override val overrideLiveCheck: Boolean = false) extends SendClientMessage
  case class SendClientKillCursorsMessage(override val message: KillCursorsMessage, override val overrideLiveCheck: Boolean = false) extends SendClientMessage
  case class SendClientSingleWriteMessage(override val message: MongoClientWriteMessage, override val concern: WriteConcern, override val overrideLiveCheck: Boolean = false) extends SendClientWriteMessage
  case class SendClientBatchWriteMessage(override val message: MongoClientWriteMessage, override val concern: WriteConcern, override val overrideLiveCheck: Boolean = false) extends SendClientWriteMessage

  // Messages we can send
  sealed trait Outgoing
  sealed trait Failure extends Outgoing {
    val exception: Throwable
  }
  // failure of a single query (connection is fine)
  case class QueryFailure(override val exception: Throwable) extends Failure
  // exception indicating we are hosed (e.g. channel closed)
  case class ConnectionFailure(override val exception: Throwable) extends Failure

  // These replies are on success, we send a failure message otherwise
  case class CheckMasterReply(isMaster: Boolean, maxBSONObjectSize: Int) extends Outgoing
  case class GetMoreReply(cursorID: Long, documents: Seq[Array[Byte]]) extends Outgoing
  case class CursorReply(cursorActor: ActorRef) extends Outgoing
  case class SingleDocumentReply(document: Array[Byte]) extends Outgoing
  case class OptionalSingleDocumentReply(maybeDocument: Option[Array[Byte]]) extends Outgoing
  case class WriteReply(id: Option[AnyRef], result: WriteResult) extends Outgoing
  case class BatchWriteReply(ids: Option[Seq[AnyRef]], result: WriteResult) extends Outgoing

  protected[mongodb] def createCommand[Cmd <% BSONDocument](ns: String, cmd: Cmd) = {
    QueryMessage(ns + ".$cmd", 0, -1, cmd)
  }

  protected[mongodb] def buildQueryFailure(reply: ReplyMessage): QueryFailure = {
    log.trace("Query Failure")
    // Attempt to grab the $err document
    reply.documents.headOption match {
      case Some(b) => {
        val errDoc = SerializableDocument.decode(b) // TODO - Extractors!
        log.trace("Error Document found: %s", errDoc)
        QueryFailure(new Exception(errDoc.getAsOrElse[String]("$err", "Unknown Error.")))
      }
      case None => {
        log.warn("No Error Document Found.")
        QueryFailure(new Exception("Unknown error."))
      }
    }
  }

  protected[mongodb] def checkQueryFailure(reply: ReplyMessage)(buildPotentialSuccess: => Outgoing): Outgoing = {
    if (reply.queryFailure) {
      buildQueryFailure(reply)
    } else {
      buildPotentialSuccess
    }
  }

  protected[mongodb] def checkCursorFailure(reply: ReplyMessage)(buildPotentialSuccess: => Outgoing): Outgoing = checkQueryFailure(reply)({
    if (reply.cursorNotFound) {
      log.warn("Cursor Not Found.")
      QueryFailure(new Exception("Cursor Not Found"))
    } else {
      buildPotentialSuccess
    }
  })

  protected[mongodb] def parseCheckMasterReply(reply: ReplyMessage): (Boolean, Int) = {
    val doc = implicitly[SerializableBSONObject[Document]].decode(reply.documents.head)
    log.debug("Got a result from isMaster command: %s", doc)
    val isMaster = doc.getAsOrElse[Boolean]("ismaster", false)
    val maxBSONObjectSize = doc.getAsOrElse[Int]("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize)
    (isMaster, maxBSONObjectSize)
  }

  protected[mongodb] def buildCheckMasterReply(reply: ReplyMessage): Outgoing = checkQueryFailure(reply)({
    log.trace("building CheckMaster reply.")
    val (isMaster, maxBSONObjectSize) = parseCheckMasterReply(reply)
    CheckMasterReply(isMaster, maxBSONObjectSize)
  })

  protected[mongodb] def buildGetMoreReply(reply: ReplyMessage): Outgoing = checkCursorFailure(reply)({
    log.trace("building Get More reply.")
    GetMoreReply(reply.cursorID, reply.documents)
  })

  protected[mongodb] def buildCursorReply(connectionActor: ActorRef, namespace: String, reply: ReplyMessage): Outgoing = checkCursorFailure(reply)({
    log.trace("building Cursor Request reply.")
    val cursorActor = Actor.actorOf(new CursorActor(connectionActor,
      reply.cursorID,
      namespace,
      reply.startingFrom,
      reply.documents)).start

    CursorReply(cursorActor)
  })

  protected[mongodb] def buildSingleDocumentReply(reply: ReplyMessage): Outgoing = checkCursorFailure(reply)({
    log.trace("building Single Document reply.")
    // This may actually be better as a disableable assert but for now i want it hard.
    require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a SingleDocumentReply.")
    val doc = reply.documents.head
    SingleDocumentReply(doc)
  })

  protected[mongodb] def buildOptionalSingleDocumentReply(reply: ReplyMessage): Outgoing = checkQueryFailure(reply)({
    log.trace("building Optional Single Document reply.")
    require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete an OptionalSingleDocumentReply.")
    val doc = reply.documents.headOption
    OptionalSingleDocumentReply(doc)
  })

  protected[mongodb] def buildWriteReply(reply: ReplyMessage): Outgoing = checkQueryFailure(reply)({
    log.trace("building WriteReply.")
    buildBatchWriteReply(reply) match {
      case BatchWriteReply(maybeIds, result) =>
        WriteReply(maybeIds flatMap { _.headOption }, result)
      case f: Failure =>
        f
      case _ =>
        throw new Error("This is not supposed to happen, buildBatchWriteReply returned something weird.")
    }
  })

  protected[mongodb] def buildBatchWriteReply(reply: ReplyMessage): Outgoing = checkQueryFailure(reply)({
    log.trace("building BatchWriteReply.")
    require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete this write reply.")
    // Attempt to grab the document
    reply.documents.headOption match {
      case Some(b) => {
        val doc = SerializableDocument.decode(b) // TODO - Extractors!
        log.debug("Document found: %s", doc)
        val ok = boolCmdResult(doc, false)
        // this is how the Java driver decides to throwOnError, !ok || "err"
        val failed = !ok || doc.get("err").isDefined
        if (failed) {
          // when does mongodb use errmsg vs. err ?
          val errmsg = doc.getAsOrElse[String]("errmsg", doc.getAs[String]("err").get)
          // FIXME should be a custom exception type probably
          QueryFailure(new Exception(errmsg))
        } else {
          val w = WriteResult(ok = true,
            error = None,
            // FIXME is it possible to have a code but no error?
            code = doc.getAs[Int]("code"),
            n = doc.getAsOrElse[Int]("n", 0),
            upsertID = doc.getAs[AnyRef]("upserted"),
            updatedExisting = doc.getAs[Boolean]("updatedExisting"))
          log.debug("W: %s", w)
          // FIXME obviously this doesn't work if there are multiple IDs.
          // need to look up how that shows up in the protocol.
          BatchWriteReply(Some(Seq(w.upsertID)), w)
        }
      }
      case None => {
        log.warn("No Document Found.")
        QueryFailure(new Exception("Unknown error; no document returned.."))
      }
    }
  })
}
