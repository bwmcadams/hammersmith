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

import org.bson.BSONSerializer

/**
 * OP_UPDATE Message
 *
 * OP_UPDATE is used to update a document in a given collection.
 *
 * There is no server response to OP_UPDATE
 * you must use getLastError()
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPUPDATE
 */
trait UpdateMessage extends MongoMessage {
  // val header: MessageHeader // standard message header
  val opCode = OpCode.OpUpdate

  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  def flags: Int = { // bit vector of UpdateFlags assembled from UpdateFlag
    var _f = 0
    if (upsert) _f |= UpdateFlag.Upsert.id
    if (multiUpdate) _f |= UpdateFlag.MultiUpdate.id
    _f
  }

  val upsert: Boolean
  val multiUpdate: Boolean

  val query: BSONDocument // The query document to select from mongo
  val update: BSONDocument // The document specifying the update to perform

  protected def writeMessage(enc: BSONSerializer) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(flags)
    // TODO - Check against Max BSON Size
    enc.putObject(query)
    enc.putObject(update)
  }
}

object UpdateFlag extends Enumeration {
  val Upsert = Value(1 << 0)
  val MultiUpdate = Value(1 << 1)
  // Bits 2-31 are reserved for future usage
}