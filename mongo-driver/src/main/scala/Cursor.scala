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
import org.bson._
import scala.collection.Iterator
import scala.collection.mutable.Queue
import java.util.concurrent.CountDownLatch
import org.jboss.netty.channel.ChannelHandlerContext
import com.mongodb.wire.{GetMoreMessage , ReplyMessage}
import com.mongodb.futures.{FutureResult , RequestFutures}
import scala.annotation.tailrec

object Cursor extends Logging {
  trait IterState
  case class Entry(doc: BSONDocument) extends IterState
  case object Empty extends IterState
  case object EOF extends IterState
  trait IterCmd
  case object Done extends IterCmd
  case class Next(op: (IterState) => IterCmd) extends IterCmd
  case class NextBatch(op: (IterState) => IterCmd) extends IterCmd

  def iterate(cursor: Cursor)(op: (IterState) => IterCmd) {
    log.debug("Iterating '%s' with op: '%s'", cursor, op)
    @tailrec def next(f: (IterState) => IterCmd): Unit = op(cursor.next()) match {
      case Done => {
        log.info("Closing Cursor.")
        cursor.close()
      }
      case Next(tOp) => {
        log.debug("Next!")
        next(tOp)
      }
      case NextBatch(tOp) => cursor.nextBatch(() => {
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
class Cursor(val namespace: String, protected val reply: ReplyMessage)
            (implicit val ctx: ChannelHandlerContext) extends Logging {

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
  def hasMore =  !cursorEmpty

  def nextBatch(notify: Function0[Unit]){
    assume(hasMore, "GetMore should not be invoked on an empty Cursor.")
    log.debug("Invoking getMore()")
    MongoConnection.send(GetMoreMessage(namespace, batchSize, cursorID),
                         RequestFutures.getMore((reply: Option[(Long, Seq[BSONDocument])], res: FutureResult) => {
      if (res.ok) {
        reply match {
          case Some((id, batch)) => {
            log.debug("Got a result from 'getMore' command (id: %d).", id)
            cursorEmpty = validCursor(id)
            docs.enqueue(batch: _*)

          }
          case None => {
            log.error("Command 'getMore' reported success but empty reply.")
            cursorEmpty = true // assume a server issue
          }
        }
      } else {
        // TODO - should we have some way of signalling an error to the callback?
        log.warning("Command 'getMore' failed: %s / Msg: %s", res, reply.getOrElse(null))
        cursorEmpty = true // assume a server issue
      }
      notify()
    }))
  }

  /**
  */
  def next() = try {
    Cursor.Entry(docs.dequeue())
  } catch {
    case nse: java.util.NoSuchElementException => {
      log.debug("No Such Element Exception")
      if (hasMore) {
        log.debug("Has More.")
        Cursor.Empty
      } else {
        log.debug("Cursor Exhuasted.")
        Cursor.EOF
      }
    }
  }

  def close() = {
    log.warning("WARNING: Close called but not currently implemented.")
  }

  def iterate = Cursor.iterate(this) _
}