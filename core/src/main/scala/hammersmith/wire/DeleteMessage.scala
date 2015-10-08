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

package hammersmith
package wire

import hammersmith.collection._
import hammersmith.bson.{ImmutableBSONDocumentComposer, SerializableBSONObject}
import hammersmith.util.Logging
import akka.util.ByteString

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
abstract class DeleteMessage extends MongoClientWriteMessage {
  type D
  val dM: SerializableBSONObject[D]
  // jval header: MessageHeader // Standard message header
  val opCode = OpCode.OpDelete
  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  def flags: Int = { // bit vector of delete flags assembled from DeleteFlag
    var _f = 0
    if (removeSingle) _f |= DeleteFlag.SingleRemove.id
    _f
  }

  val removeSingle: Boolean // Remove only first matching document

  val query: D // Query object for what to delete

  def ids: Seq[Option[AnyRef]] = List(None)

  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int) = {
    val b = ByteString.newBuilder
    b.putInt(ZERO) // 0 - reserved for future use (stupid protocol design *grumble grumble*)
    ImmutableBSONDocumentComposer.composeCStringValue(namespace)(b) // "dbname.collectionname"
    b.putInt(flags) // bit vector - see DeleteFlag
    ImmutableBSONDocumentComposer.composeBSONObject(None /* field name */, dM.iterator(query))(b)

    b.result()
  }
}

object DeleteMessage extends Logging {
  def apply[T: SerializableBSONObject](ns: String, q: T, onlyRemoveOne: Boolean = false)(writeConcern: WriteConcern = WriteConcern.Safe) =
    new DefaultDeleteMessage[T](ns, q, onlyRemoveOne)(writeConcern)
}

sealed class DefaultDeleteMessage[T: SerializableBSONObject](val namespace: String,
                                                             val query: T,
                                                             val removeSingle: Boolean)(val writeConcern: WriteConcern)
  extends DeleteMessage {
  type D = T
  val dM: SerializableBSONObject[D] = implicitly[SerializableBSONObject[T]]
}

object DeleteFlag extends Enumeration {
  /**
   * If set, remove only first matching doc.
   * Default behavior removes all matching documents.
   */
  val SingleRemove = Value(1 << 0)

  // Bits 1-31 are reserved for future usage
}
