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
import scala.collection.IterableLike
import com.mongodb.wire.ReplyMessage
import org.bson.{BSONDocument , BSONDeserializer}

/**
 * Lazy decoding Cursor for MongoDB Objects
 * Works on the idea that we're better off decoding each
 * message as iteration occurs rather than trying to decode them
 * all up front
 * TODO - Generic version with type passing
 */
class Cursor(protected val reply: ReplyMessage) extends Stream[BSONDocument] with Logging {
  val cursorID: Long = reply.cursorID

  /**
   * Mutable internally as we push further through the cursor on the server
   */
  protected var _startIndex = reply.startingFrom

  protected var _docs = Stream(reply.documents: _*)

  protected var hasMore = true

  override def isEmpty = !hasMore

  override def head = _docs.head

  override def tail = _docs.tail

  def tailDefined: Boolean = {
    val defined = !tail.isEmpty
    log.debug("TailDefined called. Tail: %s Defined? %s", tail, defined)
    // TODO Fetch more results from network, but can we do it in a way that is non blocking?
    // Stream has defined forEach as final so I can't override in anyway to defer... may have to move to LinearSeq
    defined
  }

  log.debug("Initializing a new cursor with cursorID: %d, startIndex: %d, initialItems:  %d / %s", cursorID, _startIndex, _docs.size, _docs)

  /**
   * Iterates the cursor, fetching more results as needed while still being presumably async.
   */
//  override def foreach[B](f: (BSONDocument) => B) {
//    log.debug("Iterating via foreach on Cursor with %s", f)
//    _docs.foreach((doc: BSONDocument) => {
//      f(doc)
//    })
//  }

}