/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
package codes.bytes.hammersmith.akka.wire

import akka.util.ByteString
import codes.bytes.hammersmith.akka.bson.{SerializableBSONObject, ImmutableBSONDocumentComposer}
import codes.bytes.hammersmith.wire.WriteConcern

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

  def flags: Int = {
    // bit vector of insert flags assembled from InsertFlag
    var _f = 0
    if (continueOnError) _f |= InsertFlag.ContinueOnError.id
    _f
  }

  val namespace: String
  // Full collection name (dbname.collectionname)
  val documents: Seq[T]
  // One or more documents to insert into the collection
  val continueOnError: Boolean // continue if an error occurs on bulk insert

  def ids: Seq[Option[Any]] = documents.map(tM._id(_))

  /**
    * Message specific implementation.
    *
    * serializeHeader() writes the header, serializeMessage does a message
    * specific writeout
    */
  protected def serializeMessage()(implicit maxBSON: Int) = {
    val b = ByteString.newBuilder
    b.putInt(flags) // bit vector - see InsertFlag
    ImmutableBSONDocumentComposer.composeCStringValue(namespace)(b) // "dbname.collectionname"
    documents foreach { doc =>
      ImmutableBSONDocumentComposer.composeBSONObject(None /* field name */ , tM.iterator(doc))(b)
    }

    b.result()
  }
}

/**
  * Insert for a single document
  */
abstract class SingleInsertMessage(val namespace: String) extends InsertMessage

sealed class DefaultSingleInsertMessage[DocType: SerializableBSONObject](override val namespace: String,
                                                                         val doc: DocType
                                                                        )(val writeConcern: WriteConcern)
  extends SingleInsertMessage(namespace) {
  type T = DocType
  val tM = implicitly[SerializableBSONObject[T]]
  val continueOnError = false
  val documents = Seq(doc)
}

/**
  * Insert for multiple documents
  *
  */
abstract class BatchInsertMessage(val namespace: String) extends InsertMessage

sealed class DefaultBatchInsertMessage[DocType: SerializableBSONObject](override val namespace: String,
                                                                        val continueOnError: Boolean,
                                                                        val documents: Seq[DocType]
                                                                       )(val writeConcern: WriteConcern)
  extends BatchInsertMessage(namespace) {
  type T = DocType
  val tM = implicitly[SerializableBSONObject[T]]
}

object InsertMessage {
  def apply[DocType: SerializableBSONObject](ns: String, continueOnError: Boolean, docs: DocType*)(writeConcern: WriteConcern = WriteConcern.Safe) = {
    assume(docs.nonEmpty, "Cannot insert 0 documents.")
    if (docs.length > 1) {
      new DefaultBatchInsertMessage[DocType](ns, continueOnError, docs)(writeConcern)
    } else {
      new DefaultSingleInsertMessage[DocType](ns, docs.head)(writeConcern)
    }
  }
}

object InsertFlag extends Enumeration {
  /**
    * If set, the database will not stop processing a bulk insert
    * if one fails (eg due to duplicate IDs). This makes bulk insert
    * behave similarly to a series of single inserts,
    * except lastError will be set if any insert fails, not just the last one.
    * If multiple errors occur, only the most recent will be reported by getLastError. (new in 1.9.1)
    */
  val ContinueOnError = Value(1 << 0)

  // Bits 1-31 are reserved for future usage
}
