/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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

import org.bson._
import hammersmith.collection._
import hammersmith.collection.BSONDocument
import hammersmith.bson.{BSONSerializer, SerializableBSONObject}
import hammersmith.util.Logging

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
  type Upd

  implicit val qM: SerializableBSONObject[Q]
  implicit val uM: SerializableBSONObject[Upd]

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

  val update: Upd // The document specifying the update to perform

  // TODO - Can we actually get some useful info here?
  def ids: Seq[Option[AnyRef]] = List(None)

  // todo - fix me
  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(flags)
    // TODO - Check against Max BSON Size
    //enc.putObject(query)
    // todo - fix me
    //enc.encodeObject(implicitly[SerializableBSONObject[Upd]].compose(update))
  }

  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int) = ???
}

abstract class BatchUpdateMessage(val namespace: String, val upsert: Boolean = false) extends UpdateMessage { val multiUpdate = true }

abstract class SingleUpdateMessage(val namespace: String, val upsert: Boolean = false) extends UpdateMessage { val multiUpdate = false }

object UpdateMessage extends Logging {
  def apply[QT: SerializableBSONObject, UpdT: SerializableBSONObject](ns: String, q: QT, updateSpec: UpdT, _upsert: Boolean = false, multi: Boolean = false) = {
    if (multi)  new BatchUpdateMessage(ns, _upsert) {
        type Q = QT
        type Upd = UpdT
        val query = q
        val qM = implicitly[SerializableBSONObject[QT]]
        val update = updateSpec
        val uM = implicitly[SerializableBSONObject[UpdT]]
    }
    else new SingleUpdateMessage(ns, _upsert) {
        type Q = QT
        type Upd = UpdT
        val query = q
        val qM = implicitly[SerializableBSONObject[QT]]
        val update = updateSpec
        val uM = implicitly[SerializableBSONObject[UpdT]]
    }
  }



}

object UpdateFlag extends Enumeration {
  val Upsert = Value(1 << 0)
  val MultiUpdate = Value(1 << 1)
  // Bits 2-31 are reserved for future usage
}
