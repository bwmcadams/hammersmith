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
import scala.annotation.tailrec
import akka.actor._
import akka.dispatch._

object Cursor extends Logging {
  trait IterState
  case class Entry[T: SerializableBSONObject](doc: T) extends IterState
  // FIXME Empty is just a leftover now since there's no manual NextBatch
  case object Empty extends IterState
  case object EOF extends IterState
  trait IterCmd
  case object Done extends IterCmd
  case class Next(op: (IterState) => IterCmd) extends IterCmd
  case class NextBatch(op: (IterState) => IterCmd) extends IterCmd

  def apply[T: SerializableBSONObject](cursorActor: ActorRef): Cursor[T] = {
    log.debug("Instantiate new Cursor[%s]", implicitly[SerializableBSONObject[T]])
    try {
      new Cursor[T](cursorActor)
    } catch {
      case e => {
        log.error(e, "*****EXCEPTION IN CURSOR INSTANTIATE: %s ****", e.getMessage)
        throw e
      }
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
      // FIXME NextBatch is just a leftover now
      case NextBatch(tOp) => {
        log.trace("NextBatch (just treating the same as Next)")
        next(tOp)
      }
    }
    next(op)
  }
}

/**
 * Cursor for MongoDB
 *
 * FIXME this could now just derive from Iterator[Future[T]] and add close() and batchSize.
 * There's no need for a custom API like this, since NextBatch is now automatic... could drop a lot of code.
 */
class Cursor[T: SerializableBSONObject](private val cursorActor: ActorRef) extends Logging {

  // Go ahead and ask the actor for an entry
  private var nextEntryFuture: Option[Future[Any]] = Some(cursorActor !!! CursorActor.Next)

  // batch size is cached locally and also sent to the actor
  private var batch = 0

  def batchSize = batch
  def batchSize_=(size: Int) {
    batch = size
    cursorActor ! CursorActor.SetBatchSize(size)
  }

  // Whether or not we've received EOF
  def hasMore = nextEntryFuture.isDefined
  def isEmpty = !hasMore

  /**
   */
  def next(): Cursor.IterState = {
    val decoder = implicitly[SerializableBSONObject[T]]
    log.trace("NEXT: %s ", decoder.getClass)

    nextEntryFuture match {
      case Some(f) =>
        f.get match {
          case CursorActor.Entry(bytes) =>
            // kick off request for next one
            nextEntryFuture = Some(cursorActor !!! CursorActor.Next)

            // return this one
            val doc = decoder.decode(bytes)
            Cursor.Entry(doc)
          case CursorActor.EOF =>
            nextEntryFuture = None
            Cursor.EOF
        }
      case None =>
        Cursor.EOF
    }
  }

  // FIXME drop this once we don't need the compat
  def nextBatch(notify: Function0[Unit]) {
    nextEntryFuture match {
      case Some(f) =>
        f.onComplete({ future => notify.apply() })
      case None =>
        notify.apply()
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
    log.debug("%s stopping cursor actor: %s", this, cursorActor)

    cursorActor.stop()
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
