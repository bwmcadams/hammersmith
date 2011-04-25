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
import org.bson.collection.{Document , BSONDocument}

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
trait InsertMessage extends MongoClientWriteMessage {
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpInsert

  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val documents: Seq[BSONDocument] // One or more documents to insert into the collection

  protected def writeMessage(enc: BSONSerializer) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    // TODO - Limit batch insert size which should be 4 * MaxBSON
    for (doc <- documents) enc.putObject(doc)

  }
}

object InsertMessage extends Logging {
  def apply(ns: String, docs: Seq[Document]) = new InsertMessage {
    val namespace = ns
    val documents = docs
  }
}