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
import org.jboss.netty.channel.ChannelHandlerContext
import com.mongodb.async.wire.{ GetMoreMessage, ReplyMessage }
import com.mongodb.async.futures.RequestFutures
import com.mongodb.async.util.ConcurrentQueue
import scala.annotation.tailrec
import com.twitter.util.CountDownLatch

object Cursor extends Logging {
  trait IterState
  case class Entry[T: SerializableBSONObject](doc: T) extends IterState
  case object Empty extends IterState
  case object EOF extends IterState
  trait IterCmd
  case object Done extends IterCmd
  case class Next(op: (IterState) => IterCmd) extends IterCmd
  case class NextBatch(op: (IterState) => IterCmd) extends IterCmd

  def apply[T](namespace: String, reply: ReplyMessage)(implicit ctx: ChannelHandlerContext, decoder: SerializableBSONObject[T]) = {
    log.info("Instantiate new Cursor[%s], on namespace: '%s', # messages: %d", decoder, namespace, reply.numReturned)
    try {
      new Cursor[T](namespace, reply)(ctx, decoder)
    } catch {
      case e => log.error(e, "*****EXCEPTION IN CURSOR INSTANTIATE: %s ****", e.getMessage)
    }
  }
  /**
   * Internal helper, more or less a "default" iterator for internal usage
   * Not exposed publicly but useful as an example.
   */
  protected[mongodb] def basicIter[T: SerializableBSONObject](cursor: Cursor[T])(f: T => Unit) = {
    def next(op: Cursor.IterState): Cursor.IterCmd = op match {
      case Cursor.Entry(doc: T) => {
        f(doc)
        Cursor.Next(next)
      }
      case Cursor.Empty => {
        log.trace("Empty... Next batch.")
        Cursor.NextBatch(next)
      }
      case Cursor.EOF => {
        log.trace("EOF... Cursor done.")
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
  def iterate[T: SerializableBSONObject](cursor: Cursor[T])(op: (IterState) => IterCmd) {
    log.trace("Iterating '%s' with op: '%s'", cursor, op)
    def next(f: (IterState) => IterCmd): Unit = op(cursor.next()) match {
      case Done => {
        log.trace("Closing Cursor.")
        cursor.close()
      }
      case Next(tOp) => {
        log.trace("Next!")
        next(tOp)
      }
      case NextBatch(tOp) => cursor.nextBatch(() => {
        log.debug("Next Batch Loaded.")
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
class Cursor[T](val namespace: String, protected val reply: ReplyMessage)(implicit val ctx: ChannelHandlerContext, val decoder: SerializableBSONObject[T]) extends Logging {

  val cursorID: Long = reply.cursorID

  protected val handler = ctx.getHandler.asInstanceOf[MongoConnectionHandler]
  protected implicit val channel = ctx.getChannel
  protected implicit val maxBSONObjectSize = handler.maxBSONObjectSize // todo - will this change ? Should we explicitly grab it when needed

  /**
   * Cursor ID 0 indicates "No more results"
   * HOWEVER - Cursors can be positive OR negative
   * If we were initialized with a cursorID of 0, there are no more results
   * otherwise we'll flip this later during our getMores
   */
  protected def validCursor(id: Long) = id == 0
  protected var cursorEmpty = validCursor(cursorID)
  @volatile
  protected var gettingMore = new CountDownLatch(0)

  /**
   * Mutable internally as we push further through the cursor on the server
   */
  protected var startIndex = reply.startingFrom

  log.trace("Decode %s docs.", reply.documents.length)
  try {
    val _d = Seq.newBuilder[T]
    for (doc <- reply.documents) {
      log.trace("Decoding: %s", doc)
      try {
        val x = decoder.decode(doc)
        log.trace("Decoded: %s", x)
        _d += x
      } catch {
        case e => log.info("ERROR!!!!!!!!! %s", e)
      }
    }
    val _decoded = _d.result
    // reply.documents.map(decoder.decode)
    log.debug("Decoded %s docs: %s", _decoded.length, _decoded)
  } catch {
    case e => log.error(e, "Document decode failure: %s", e)
  }

  // TODO - Move to lazy decoding model
  protected val docs = ConcurrentQueue(reply.documents.map(decoder.decode): _*) // ConcurrentQueue(_decoded: _*)

  log.info("Initializing a new cursor with cursorID: %d, startIndex: %d, docs: %s", cursorID, startIndex, docs)

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
      log.trace("Invoking getMore(); cursorID: %s, queue size: %s", cursorID, docs.size)
      MongoConnection.send(GetMoreMessage(namespace, batchSize, cursorID),
        RequestFutures.getMore((reply: Either[Throwable, (Long, Seq[T])]) => {
          reply match {
            case Right((id, batch)) => {
              log.trace("Got a result from 'getMore' command (id: %d).", id)
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
        }))
    } else log.warn("Already gettingMore on this cursor.  May be a concurrency issue if called repeatedly.")
  }

  /**
   */
  def next() = try {
    log.trace("NEXT: %s ", decoder.getClass)
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
  protected[mongodb] def foreach(f: T => Unit) = {
    log.trace("Foreach: %s | empty? %s", f, isEmpty)
    Cursor.basicIter(this)(f)
  }

  def close() {
    log.debug("Closing out cursor: %s", this)
    /**
     * Basically if the cursorEmpty is true we can just NOOP here
     * as MongoDB automatically cleans up fully iterated cursors.
     */
    if (hasMore) {
      validCursor(0) // zero out the 'hasMore' status
      MongoConnection.killCursors(cursorID)
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
