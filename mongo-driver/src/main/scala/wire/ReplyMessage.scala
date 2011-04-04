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
package wire

import org.bson.BSONEncoder

/**
 * OP_REPLY
 *
 * OP_REPLY is sent by the database in response to an OP_QUERY or
 * OP_GET_MORE message.
 *
 */
trait ReplyMessage extends MongoMessage {
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpReply
  // TODO - Read flags in!
  val flags: Int // bit vector of reply flags, available in ReplyFlag
  def cursorNotFound = flags & ReplyFlag.CursorNotFound.id
  def queryFailure = flags & ReplyFlag.QueryFailure.id
  def awaitCapable = flags & ReplyFlag.AwaitCapable.id
  val cursorID: Long // cursorID, for client to do getMores
  val startingFrom: Int // Where in the cursor this reply starts at
  val numReturned: Int // Number of documents in the reply.
  val documents: Seq[BSONDocument] // Sequence of documents

  protected def writeMessage(enc: BSONEncoder) =
    throw new UnsupportedOperationException("This message is not capable of being written. "
      + "Replies come only from the server.")
}

object ReplyFlag extends Enumeration {
  /**
   * Set when getMore is called but the cursorID is not valid
   * on the server.  Returns with 0 results.
   */
  val CursorNotFound = Value(1 << 0)

  /**
   * Set when a query fails.  Results consist of one document
   * containing an "$err" field describing the failure.
   */
  val QueryFailure = Value(1 << 1)

  /**
   * Drivers should ignore this; only mongos ever sees it set,
   * in which case it needs to update config from the server
   */
  val ShardConfigState = Value(1 << 2)

  /**
   * Set when the server supports the AwaitData Query Option.  If
   * it doesn't, a client should sleep a little between getMore's on
   * a Tailable cursor.  MongoDB 1.6+ support AwaitData and thus always
   * sets AwaitCapable
   */
  val AwaitCapable = Value(1 << 3)

  // Bits 4-31 are reserved for future usage
}

