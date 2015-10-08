/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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

package codes.bytes.hammersmith
package wire

import codes.bytes.hammersmith.util.Logging
import akka.util.ByteString
import codes.bytes.hammersmith.bson.ImmutableBSONDocumentComposer

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
  val numCursors: Int = cursorIDs.length // The number of cursorIDs in the message
  def cursorIDs: Seq[Long] // Sequence of cursorIDs to close


  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int) = {
    val b = ByteString.newBuilder
    b.putInt(ZERO) // 0 - reserved for future use (stupid protocol design *grumble grumble*)
    b.putInt(numCursors)
    cursorIDs foreach { id =>
      b.putLong(id)
    }
    b.result()
  }

}

sealed class DefaultKillCursorsMessage(val cursorIDs: Seq[Long]) extends KillCursorsMessage


object KillCursorsMessage extends Logging {
  def apply(ids: Seq[Long], writeConcern: WriteConcern = WriteConcern.Safe) = new DefaultKillCursorsMessage(ids)

  def apply(id: Long)  = new DefaultKillCursorsMessage(Seq(id))
}
