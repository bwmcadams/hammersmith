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

import org.bson._
import org.bson.collection._
import org.bson.util.Logging

/**
 * OP_DELETE Message
 *
 * OP_DELETE is used to remove one or more documents from a collection
 *
 * There is no server response to OP_DELETE
 * you must use getLastError()
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPDELETE
 */
abstract class DeleteMessage[T: SerializableBSONObject] extends MongoClientWriteMessage {
  // val header: MessageHeader // Standard message header
  val opCode = OpCode.OpDelete
  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  def flags: Int = { // bit vector of delete flags assembled from DeleteFlag
    var _f = 0
    if (removeSingle) _f |= DeleteFlag.SingleRemove.id
    _f
  }

  val removeSingle: Boolean // Remove only first matching document

  val query: T // Query object for what to delete

  def ids: Seq[Option[AnyRef]] = List(None)

  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(flags)
    enc.encodeObject(implicitly[SerializableBSONObject[T]].encode(query))
  }
}

object DeleteMessage extends Logging {
  def apply[T: SerializableBSONObject](ns: String, q: T, onlyRemoveOne: Boolean = false) = new DeleteMessage {
    val namespace = ns
    val query = q
    val removeSingle = onlyRemoveOne
  }
}

object DeleteFlag extends Enumeration {
  /**
   * If set, remove only first matching doc.
   * Default behavior removes all matching documents.
   */
  val SingleRemove = Value(1 << 0)

  // Bits 1-31 are reserved for future usage
}
