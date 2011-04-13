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

import org.bson.util.Logging
import com.mongodb.wire.ReplyMessage
import org.bson._
import scala.collection.Iterator
import scala.collection.mutable.Queue
import java.util.concurrent.CountDownLatch

/**
 * Cursor for MongoDB
 *
 * Currently, using next() will block when it needs to do a getmore.
 * If you want a more 'futured' non-blocking behavior use the foreach, etc. methods which will delay calling back.
 * TODO - Generic version with type passing
 */
class Cursor(protected val reply: ReplyMessage) extends Iterator[BSONDocument] with Logging {
  val cursorID: Long = reply.cursorID

  /**
  * Cursor ID 0 indicates "No more results"
  * HOWEVER - Cursors can be positive OR negative
  */
  protected var cursorEmpty = false

  /**
   * Mutable internally as we push further through the cursor on the server
   */
  protected var startIndex = reply.startingFrom

  protected val docs = Queue(reply.documents: _*)

  log.debug("Initializing a new cursor with cursorID: %d, startIndex: %d, initialItems:  %d / %s", cursorID, startIndex,
                                                                                                   docs.size, docs)

  override def isTraversableAgain = false // Too much hassle in "reiterating"

  // Whether or not there are more docs *on the server*
  protected def hasMore =  !cursorEmpty

  def hasNext = {
    /**
     * Possibly a bit tricky
     * As we're looking for:
     *  a) Are there more docs in the stream CURRENTLY
     *  b) AND, if not, are there possibly more available on the server?
     */
    if (docs.length > 0) {
      log.trace("Still docs in the queue.  Has Next.")
      true
    } else if (hasMore) {
      log.trace("Queue is empty but non-zero cursor ID.  Will need to fetch more.")
      getMore()
      true
    } else {
      log.trace("Empty queue, zeroed cursorID.  No More Next.")
      false
    }
  }

  protected var gettingMore = new CountDownLatch(0)

  protected def getMore() = docs.synchronized {
    if (gettingMore.getCount > 0) {
      log.warn("GetMore called while Latch is set.  Ignoring GetMore call.")
    } else {
      gettingMore = new CountDownLatch(1)
      log.trace("Invoking getMore()")
    }
  }
  /**
  * WARNING - Currently blocks during getMore  Be careful.
  * TODO - I think we HAVE To block here for someone treating us like an iterator...
  */
  def next() = {
    if (docs.length ==  0 && hasMore) {
      log.trace("Waiting for more from the server.")
      log.warn("Blocking on the getMore op.")
      gettingMore.await()
    }
    log.trace("Next: Have docs, dequeueing.")
    docs.dequeue()
  }

  /**
   * Iterates the cursor, fetching more results as needed while still being presumably async.
   */
//  override def foreach[B](f: (BSONDocument) => B) {
//    log.debug("Iterating via foreach on Cursor with %s", f)
//    docs.foreach((doc: BSONDocument) => {
//      f(doc)
//    })
//  }

}