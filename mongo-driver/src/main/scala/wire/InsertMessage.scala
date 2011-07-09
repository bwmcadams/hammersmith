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
import org.bson.util.Logging
import org.bson.collection.{ Document, BSONDocument }
import scala.collection.mutable.Queue

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
abstract class InsertMessage[T: SerializableBSONObject] extends MongoClientWriteMessage {
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpInsert

  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val documents: Seq[T] // One or more documents to insert into the collection
  val m = implicitly[SerializableBSONObject[T]]

  def ids: Seq[Option[AnyRef]] = documents.map(m._id(_))

  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    /**
     * The limit for batch insert is 4 x MaxBSON
     */
    log.debug("Docs Length: %s", documents.length)
    val q = Queue(documents: _*)
    // TODO - test recursion
    for (doc <- q) {
      val total = enc.size
      val n = enc.encodeObject(implicitly[SerializableBSONObject[T]].encode(doc))
      log.debug("Total: %d, Last Doc Size: %d", total, n)
      // If we went over the size, backtrack and start a new message
      if (total >= (4 * maxBSON)) {
        log.info("Exceeded MaxBSON [%s] (total: %s), kicking in a new batch.", maxBSON, total)
        enc.seek(-n)
        /* TODO - This recursion may be bad and wonky... */
        InsertMessage(namespace, (doc +: q): _*).build(enc)
      }
    }

  }
}

object InsertMessage extends Logging {
  def apply[T: SerializableBSONObject](ns: String, docs: T*) = new InsertMessage {
    val namespace = ns
    val documents = docs
  }
}
