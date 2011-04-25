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


import org.bson.util.Logging
import org.bson._
import org.bson.collection._
import scala.collection.mutable.Queue
import org.jboss.netty.channel.ChannelHandlerContext
import com.mongodb.async.wire.{ GetMoreMessage, ReplyMessage }
import com.mongodb.async.futures.RequestFutures
import scala.annotation.tailrec
import com.twitter.util.CountDownLatch

object Cursor extends Logging {
  trait IterState
  case class Entry(doc: BSONDocument) extends IterState
  case object Empty extends IterState
  case object EOF extends IterState
  trait IterCmd
  case object Done extends IterCmd
  case class Next(op: (IterState) => IterCmd) extends IterCmd
  case class NextBatch(op: (IterState) => IterCmd) extends IterCmd

  /**
   * Internal helper, more or less a "default" iterator for internal usage
   * Not exposed publicly but useful as an example.
   */
  protected[mongodb] def basicIter(cursor: Cursor)(f: BSONDocument => Unit) = {
    def next(op: Cursor.IterState): Cursor.IterCmd = op match {
      case Cursor.Entry(doc) => {
        f(doc)
        Cursor.Next(next)
      }
      case Cursor.Empty => {
        log.trace("Empty... Next batch.")
        Cursor.NextBatch(next)
      }
      case Cursor.EOF => {
        log.info("EOF... Cursor done.")
        Cursor.Done
      }
    }
    iterate(cursor)(next)
  }


  /**
   * Helper for the Iteratee Pattern.
   * This is considered the safest/most canonical way to iterate any cursor.
   * Invocation of 'next' on an instance of Cursor returns one of three instances of Cursor.IterState; you need to
   * return a Cursor.IterCmd which instructs the control loop what operation to perform next.
   *    - Cursor.Element(Document instance) which contains a single document representing a successful forward iteration.
   *      Typically you should return CursorState.Next() with a callback function (usually the callback you are currently in,
   *      hello recursion!) from this, which instructs the control loop to retrieve the next document.
   *    - Cursor.Empty indicates that the current *BATCH* of the cursor is empty but more results exist on the server.
   *      The typical response to this is CursorState.NextBatch() with a callback function (again, usually a copy of the same
   *      callback you're currently in) which instructs the control loop to request the next batch from the server (OP_GET_MORE)
   *      and continue iterating.  If the getMore succeeds your callback will next be invoked with a Cursor.Element instance.
   *    - Cursor.EOF
   *      Indicates that the cursor is entirely exhausted; all local and server side results have been returned.
   *      Note the difference between this and Cursor.Empty; EOF indicates there are NO MORE RESULTS available on the server or locally.
   *      The standard response to this should be Cursor.Done which tells the control loop to stop and shut down the Cursor.
   *      I suppose if you want to be special you could respond with something else but it probably won't work right.
   */
  def iterate(cursor: Cursor)(op: (IterState) => IterCmd) {
    log.trace("Iterating '%s' with op: '%s'", cursor, op)
    @tailrec
    def next(f: (IterState) => IterCmd): Unit = op(cursor.next()) match {
      case Done => {
        log.info("Closing Cursor.")
        cursor.close()
      }
      case Next(tOp) => {
        log.trace("Next!")
        next(tOp)
      }
      case NextBatch(tOp) =>
        cursor.nextBatch(() => {
          log.info("Next Batch Loaded.")
          next(tOp)
        })
    }
    next(op)
  }
}

/**
 * Cursor for MongoDB
 *
 * Currently, using next() will block when it needs to do a getmore.
 * If you want a more 'futured' non-blocking behavior use the foreach, etc. methods which will delay calling back.
 * TODO - Generic version with type passing
 */
class Cursor(val namespace: String, protected val reply: ReplyMessage)(implicit val ctx: ChannelHandlerContext) extends Logging {

  type DocType = BSONDocument

  val cursorID: Long = reply.cursorID

  protected val handler = ctx.getHandler.asInstanceOf[MongoConnectionHandler]
  protected implicit val channel = ctx.getChannel
  protected implicit val maxBSONObjectSize = handler.maxBSONObjectSize // todo - will this change ? Should we explictily grab it when needed

  /**
   * Cursor ID 0 indicates "No more results"
   * HOWEVER - Cursors can be positive OR negative
   * If we were initialized with a cursorID of 0, there are no more results
   * otherwise we'll flip this later during our getMores
   */
  protected def validCursor(id: Long) = id == 0
  protected var cursorEmpty = validCursor(cursorID)
  @volatile protected var gettingMore = new CountDownLatch(0)

  /**
   * Mutable internally as we push further through the cursor on the server
   */
  protected var startIndex = reply.startingFrom

  protected val docs = Queue(reply.documents: _*)

  log.debug("Initializing a new cursor with cursorID: %d, startIndex: %d", cursorID, startIndex)

  /**
   * Batch size; defaults to 0 which lets mongo control the size
   */
  protected var batch = 0

  def batchSize = batch
  def batchSize_=(size: Int) { batch = size }

  // Whether or not there are more docs *on the server*
  def hasMore = !cursorEmpty
  def isEmpty = docs.length == 0 && cursorEmpty

  def nextBatch(notify: Function0[Unit]) {
    if (gettingMore.isZero) {
      gettingMore = new CountDownLatch(1)
      assume(hasMore, "GetMore should not be invoked on an empty Cursor.")
      log.debug("Invoking getMore(); cursorID: %s, queue size: %s", cursorID, docs.size)
      MongoConnection.send(GetMoreMessage(namespace, batchSize, cursorID),
        RequestFutures.getMore((reply: Either[Throwable, (Long, Seq[BSONDocument])]) => {
          reply match {
            case Right((id, batch)) => {
              log.debug("Got a result from 'getMore' command (id: %d).", id)
              cursorEmpty = validCursor(id)
              docs.enqueue(batch: _*)
            }
            case Left(t) => {
              // TODO - should we have some way of signalling an error to the callback?
              log.error(t, "Command 'getMore' failed.")
              cursorEmpty = true // assume a server issue
            }
          }
          gettingMore.countDown()
          notify()
        })
      )
    } else log.warn("Already gettingMore on this cursor.  May be a concurrency issue if called repeatedly.")
  }

  /**
   * TODO - It is probably significantly less costly time and resource wise to test length
   * instead of catching a NoSuchElement
   */
  def next() = try {
    if (docs.length > 0) Cursor.Entry(docs.dequeue()) else if (hasMore) Cursor.Empty
    else
      Cursor.EOF
  } catch { // just in case
    case nse: java.util.NoSuchElementException => {
      log.debug("No Such Element Exception")
      if (hasMore) {
        log.debug("Has More.")
        Cursor.Empty
      } else {
        log.debug("Cursor Exhausted.")
        Cursor.EOF
      }
    }
  }


  def iterate = Cursor.iterate(this) _

  /**
   * Internal use only foreach method.
   * NOT YOURS! GET YOUR OWN!
   * Seriously though, for safety reasons I didn't expose this publicly
   * because using it without understanding it can be dangerous.
   * AKA - If you want to stick your finger in this electrical socket, you'll have
   * to build your own fork first.
   */
  protected[mongodb] def foreach(f: BSONDocument => Unit) = {
    log.debug("Foreach: %s | empty? %s", f, isEmpty)
    Cursor.basicIter(this)(f)
  }

  def close() {
    log.info("Closing out cursor: %s", this)
    /**
     * Basically if the cursorEmpty is true we can just NOOP here
     * as MongoDB automatically cleans up fully iterated cursors.
     */
    if (hasMore) {
      validCursor(0) // zero out the 'hasMore' status
      handler.killCursors(cursorID)
    }

    /**
     * Clean out any remaining items
     */
    docs.clear()
  }

  /**
   * Attempts to catch and close any uncleaned up cursors.
   */
  override def finalize() {
    log.debug("Finalizing Cursor (%s)", this)
    close()
    super.finalize()
  }

}