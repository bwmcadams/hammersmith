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
import scala.annotation.tailrec

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

  self.timeout = 60 * 1000 // 60 seconds (timeout in millis)

  /**
   * Cursor ID 0 indicates "No more results on SERVER"
   * HOWEVER - Cursors can be positive OR negative
   * If we were initialized with a cursorID of 0, there are no more results
   * otherwise we'll flip this later during our getMores
   */
  private def validCursor(id: Long) = id == 0
  private var cursorEmpty = validCursor(cursorID)

  private var getMorePending = false // are we waiting on a getMore() request to mongo
  private var sendersWhoWantMore = Queue[Channel[Any]]() // channels that have asked us for more
  private var documents = Seq[Array[Byte]](initialDocuments: _*) // documents we've gotten back

  log.debug("Initializing a new CursorActor with cursorID: %d, startIndex: %d, docs: %s", cursorID, startIndex, documents)

  private def takeDocuments() = {
    val oldDocs = documents
    documents = Seq()
    Entries(oldDocs)
  }

  // use this to send to the app, but not needed to send to
  // connection actor
  private def asyncSend(channel: Channel[Any], message: Any) = {
    // We have to do this _asynchronously_ because sending a message
    // to an Akka future synchronously invokes app callbacks.
    // If the app then called back to the cursor actor it would
    // deadlock.
    Future(channel ! message, self.timeout)(self.dispatcher)
  }

  override def receive: Receive = {
    case GetMore ⇒ {
      log.trace("Someone sent GetMore to CursorActor cursorID %s", cursorID)
      sendersWhoWantMore = sendersWhoWantMore.enqueue(self.channel)
      satisfySendersWhoWantMore()
    }
    case SetBatchSize(value) ⇒
      batchSize = value
    case ConnectionActor.GetMoreReply(replyID, replyDocs) ⇒ {
      log.trace("CursorActor got a result from 'getMore' command (id: %d).", replyID)

      // Update all our fields
      getMorePending = false
      cursorEmpty = validCursor(replyID)
      documents ++= replyDocs

      log.trace("CursorActor %s now empty=%s num docs %s after getMore",
        cursorID, cursorEmpty, documents.length)

      // maybe we can make the requesters happy now
      satisfySendersWhoWantMore()
    }
    case failure: ConnectionActor.Failure ⇒ {
      log.error("CursorActor got a failure from getMore command %s", failure.exception)
      // TDOO - This is bad and needs fixing.  We need to make sure we kill this cursor, unless we got an id of 0
      cursorEmpty = true // don't try to do anything else
      throw failure.exception // this will kill the cursor actor by throwing from receive()
    }
  }

  private def requestMore() = {
    require(!cursorEmpty)
    // we want only one getMore in flight at a time, or we won't know if we've gone off the
    // end of the cursor
    if (!getMorePending) {
      log.trace("CursorActor sending getMore() to connection actor cursorID: %s, queue size: %s",
        cursorID, documents.size)
      getMorePending = true
      connectionActor ! ConnectionActor.SendClientGetMoreMessage(GetMoreMessage(namespace, batchSize, cursorID))
    }
  }

  @tailrec
  private def satisfySendersWhoWantMore(): Unit = {
    if (documents.isEmpty) {
      if (cursorEmpty) {
        // we have no docs and never will. EOF everyone.
        log.trace("CursorActor %s sending EOF to all", cursorID)
        for (sender ← sendersWhoWantMore) {
          log.trace("CursorActor %s sending EOF to %s", cursorID, sender)
          asyncSend(sender, EOF)
        }
        sendersWhoWantMore = Queue()
      } else {
        // we have no docs but could get more from mongo.
        // refill documents.
        // (note this will speculatively getMore
        // before we have someone to send them to)
        log.trace("CursorActor %s requesting more because documents.isEmpty", cursorID)
        requestMore()
      }
    } else {
      if (sendersWhoWantMore.isEmpty) {
        // nobody to satisfy, and we already have some documents
        // if someone turns up, so do nothing
        log.trace("CursorActor %s has %s documents and nobody to give them to", cursorID, documents.length)
      } else {
        // give the first sender what we have
        val (replyTo, newQueue) = sendersWhoWantMore.dequeue
        sendersWhoWantMore = newQueue
        log.trace("CursorActor %s has %s documents, giving them to first pending sender %s", cursorID, documents.length, replyTo)
        asyncSend(replyTo, takeDocuments())

        // we may need a new batch for remaining senders, or may
        // need to send them EOF, so recurse.
        satisfySendersWhoWantMore()
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
  case object GetMore extends Incoming
  case class SetBatchSize(value: Int) extends Incoming

  // Messages we can send
  sealed trait Outgoing
  case class Entries(docs: Seq[Array[Byte]]) extends Outgoing
  case object EOF extends Outgoing
}
