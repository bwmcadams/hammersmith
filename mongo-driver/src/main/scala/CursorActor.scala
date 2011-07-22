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
import akka.dispatch._
import com.mongodb.async.wire._
import org.bson._
import scala.collection.immutable.Queue
import org.bson.util.Logging

/**
 * This is an internal actor class used to encapsulate asynchronous cursor
 * traversal. It's wrapped by a public Cursor class.
 */
private[mongodb] class CursorActor(private val connectionActor: ActorRef,
  private val cursorID: Long,
  private val namespace: String,
  private var startIndex: Int,
  initialDocuments: Seq[Array[Byte]], // note, not a val - don't want to keep it around, might be huge
  private var batchSize: Int = 0)
  extends Actor
  with Logging {

  import CursorActor._

  /**
   * Cursor ID 0 indicates "No more results"
   * HOWEVER - Cursors can be positive OR negative
   * If we were initialized with a cursorID of 0, there are no more results
   * otherwise we'll flip this later during our getMores
   */
  private def validCursor(id: Long) = id == 0
  private var cursorEmpty = validCursor(cursorID)

  private var documents = Queue[Array[Byte]](initialDocuments: _*)

  log.debug("Initializing a new CursorActor with cursorID: %d, startIndex: %d, docs: %s", cursorID, startIndex, documents)

  override def receive: Receive = {
    case Next => {
      if (cursorEmpty) {
        self.reply(EOF)
      } else if (documents.isEmpty) {
        fetchMore(self.channel)
        // recursively try again.
        receive.apply(Next)
      } else {
        val (doc, newQueue) = documents.dequeue
        documents = newQueue
        self.reply(Entry(doc))
      }
    }
    case SetBatchSize(value) =>
      batchSize = value
  }

  private def fetchMore(replyTo: Channel[Any]) = {
    log.trace("Invoking getMore(); cursorID: %s, queue size: %s", cursorID, documents.size)

    val f: Future[_] = connectionActor !!! ConnectionActor.SendClientGetMoreMessage(GetMoreMessage(namespace, batchSize, cursorID))

    // we just block. we should be in our own thread so it's not a disaster.
    // if we don't block it's too hard to queue up Next messages while we
    // wait for the batch... too hard to keep them in order and deal with
    // maybe having to GetMore *again* while we haven't yet gone through all
    // the Next
    f.get match {
      case ConnectionActor.GetMoreReply(replyID, replyDocs) => {
        log.trace("Got a result from 'getMore' command (id: %d).", replyID)
        cursorEmpty = validCursor(replyID)
        replyDocs foreach { docBytes =>
          documents = documents.enqueue(docBytes)
        }
      }
      case failure: ConnectionActor.Failure => {
        log.error("getMore command failed %s", failure.exception)
        throw failure.exception // this will kill the cursor actor by throwing from receive()
      }
    }
  }

  override def postStop() = {
    log.trace("Closing cursor id %s", cursorID)
    /* MongoDB automatically cleans up fully iterated cursors. */
    if (!cursorEmpty) {
      // Is this fine? We could also do it in a Scheduler.scheduleOnce() timeout if the actor
      // hangs out too long, and we could send the cursorID to the connectionActor
      // to be batched up if it's important to batch up cursor kills
      connectionActor ! ConnectionActor.SendClientKillCursorsMessage(KillCursorsMessage(Seq(cursorID)))
    }
  }
}

object CursorActor {
  // Messages we can handle
  sealed trait Incoming
  case object Next extends Incoming
  case class SetBatchSize(value: Int) extends Incoming

  // Messages we can send
  sealed trait Outgoing
  case class Entry(doc: Array[Byte]) extends Outgoing
  case object EOF extends Outgoing
}
