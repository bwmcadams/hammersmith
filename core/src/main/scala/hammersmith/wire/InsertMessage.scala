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

import scala.collection.mutable.Queue
import bson.SerializableBSONObject
import hammersmith.util.Logging

/**
 * OP_INSERT Message
 *
 * OP_INSERT is used to insert one or more documents into a collection.
 *
 * There is no server response to OP_INSERT
 * you must use getLastError()
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPINSERT
 */
abstract class InsertMessage extends MongoClientWriteMessage {
  type T
  implicit val tM: SerializableBSONObject[T]

  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpInsert

  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val documents: Seq[T] // One or more documents to insert into the collection

  def ids: Seq[Option[Any]] = documents.map(tM._id(_))

  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int) = ???
}

/**
 * Insert for a single document
 */
abstract class SingleInsertMessage(val namespace: String) extends InsertMessage

/**
 * Insert for multiple documents
 *
 */
abstract class BatchInsertMessage(val namespace: String) extends InsertMessage

object InsertMessage extends Logging {
  def apply[DocType: SerializableBSONObject](ns: String, docs: DocType*) = {
    assume(docs.length > 0, "Cannot insert 0 documents.")
    val m = implicitly[SerializableBSONObject[DocType]]
    if (docs.length > 1) {
      new BatchInsertMessage(ns) {
        type T = DocType
        implicit val tM = m
        val documents = docs
      }
    } else {
      new SingleInsertMessage(ns) {
        type T = DocType
        implicit val tM = m
        val documents = Seq(docs.head)
      }
    }
  }
}
