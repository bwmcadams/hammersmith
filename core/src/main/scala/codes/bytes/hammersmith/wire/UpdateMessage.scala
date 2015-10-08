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

import codes.bytes.hammersmith.collection._
import codes.bytes.hammersmith.collection.BSONDocument
import codes.bytes.hammersmith.bson.{ImmutableBSONDocumentComposer, SerializableBSONObject}
import codes.bytes.hammersmith.util.Logging
import akka.util.ByteString

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
abstract class UpdateMessage extends MongoClientWriteMessage {
  type Q
  type U

  implicit val qM: SerializableBSONObject[Q]
  implicit val uM: SerializableBSONObject[U]

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

  val query: Q // The query document to select from mongo

  val update: U // The document specifying the update to perform

  // TODO - Can we actually get some useful info here?
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
    ImmutableBSONDocumentComposer.composeCStringValue(namespace)(b)
    b.putInt(flags)
    ImmutableBSONDocumentComposer.composeBSONObject(None /*field name */, qM.iterator(query))(b)
    ImmutableBSONDocumentComposer.composeBSONObject(None /*field name */, uM.iterator(update))(b)
    b.result()
  }
}

abstract class BatchUpdateMessage extends UpdateMessage {
  val multiUpdate = true
}
sealed class DefaultBatchUpdateMessage[QueryType: SerializableBSONObject,
                                       UpdateType: SerializableBSONObject](val namespace: String,
                                                                           val upsert: Boolean = false,
                                                                           val query: QueryType,
                                                                           val update: UpdateType)(val writeConcern: WriteConcern) extends BatchUpdateMessage {
  type Q = QueryType
  val qM = implicitly[SerializableBSONObject[Q]]

  type U = UpdateType

  val uM = implicitly[SerializableBSONObject[U]]

}

abstract class SingleUpdateMessage extends UpdateMessage {
  val multiUpdate = false
}
sealed class DefaultSingleUpdateMessage[QueryType: SerializableBSONObject,
                                       UpdateType: SerializableBSONObject](val namespace: String,
                                                                           val upsert: Boolean = false,
                                                                           val query: QueryType,
                                                                           val update: UpdateType)(val writeConcern: WriteConcern) extends SingleUpdateMessage {
  type Q = QueryType
  val qM = implicitly[SerializableBSONObject[Q]]

  type U = UpdateType

  val uM = implicitly[SerializableBSONObject[U]]

}

object UpdateMessage extends Logging {
  def apply[Q: SerializableBSONObject,
            U: SerializableBSONObject](ns: String,
                                       q: Q, updateSpec: U,
                                       upsert: Boolean = false,
                                       multi: Boolean = false)(writeConcern: WriteConcern = WriteConcern.Safe) = {
    if (multi)  new DefaultBatchUpdateMessage(ns, upsert, q, updateSpec)(writeConcern)
    else new DefaultSingleUpdateMessage(ns, upsert, q, updateSpec)(writeConcern)
  }

}

object UpdateFlag extends Enumeration {
  val Upsert = Value(1 << 0)
  val MultiUpdate = Value(1 << 1)
  // Bits 2-31 are reserved for future usage
}
