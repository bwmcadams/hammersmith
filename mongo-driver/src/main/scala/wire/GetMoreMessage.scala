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
 * OP_GET_MORE Message
 *
 * OP_GET_MORE is used to query the database for documents in a collection.
 *
 * The database will respond to OP_GET_MORE messages with OP_REPLY.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPGETMORE
 */
trait GetMoreMessage extends MongoClientMessage {
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpGetMore
  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val numberToReturn: Int // number of docs to return in first OP_REPLY batch
  val cursorID: Long // CursorID from the OP_REPLY (DB Genned value)

  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(numberToReturn)
    enc.writeLong(cursorID)
  }
}

object GetMoreMessage extends Logging {
  def apply(ns: String, numReturn: Int, id: Long) = new GetMoreMessage {
    val namespace = ns
    val numberToReturn = numReturn
    val cursorID = id
  }
}
