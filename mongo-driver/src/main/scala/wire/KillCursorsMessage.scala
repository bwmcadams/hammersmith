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
package wire

import org.bson.BSONSerializer
import org.bson.util.Logging

/**
 * OP_KILL_CURSORS
 *
 * OP_KILL_CURSORS is used to close an active cursor in the database,
 * to allow resources on the dbserver to be reclaimed after a query.
 *
 * Note that if a cursor is read until exhausted (read until OP_QUERY or OP_GET_MORE returns zero for the cursor id),
 * there is no need to kill the cursor.
 */
trait KillCursorsMessage extends MongoClientMessage {
  // val header: MessageHeader // Standard message header
  val opCode = OpCode.OpKillCursors
  val ZERO: Int = 0 // 0 - reserved for future use
  val numCursors: Int // The number of cursorIDs in the message
  val cursorIDs: Seq[Long] // Sequence of cursorIDs to close

  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) {
    enc.writeInt(ZERO)
    enc.writeInt(numCursors)
    for (_id <- cursorIDs) enc.writeLong(_id)
  }
}

object KillCursorsMessage extends Logging {
  def apply(ids: Seq[Long]) = new KillCursorsMessage {
    val numCursors = ids.length
    val cursorIDs = ids
  }
}
