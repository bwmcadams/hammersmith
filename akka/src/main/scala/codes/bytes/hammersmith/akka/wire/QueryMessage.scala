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
import codes.bytes.hammersmith.akka.bson.ImmutableBSONDocumentComposer
import codes.bytes.hammersmith.collection._

object QueryMessage {
  def apply(ns: String, numSkip: Int, numReturn: Int, q: BSONDocument,
            fields: Option[BSONDocument] = None, tailable: Boolean = false,
            slaveOkay: Boolean = false, disableCursorTimeout: Boolean = false, await: Boolean = false,
            exhaust: Boolean = false, partial: Boolean = false
           ) =
    new DefaultQueryMessage(ns, numSkip, numReturn, q, fields, tailable, slaveOkay,
      disableCursorTimeout, await, exhaust, partial)

}

sealed class DefaultQueryMessage(val namespace: String, val numberToSkip: Int, val numberToReturn: Int,
                                 val query: BSONDocument, val returnFields: Option[BSONDocument],
                                 val tailableCursor: Boolean, val slaveOK: Boolean,
                                 val noCursorTimeout: Boolean, val awaitData: Boolean,
                                 val exhaustData: Boolean, val partialData: Boolean
                                ) extends QueryMessage

/**
  * OP_QUERY Message
  *
  * OP_QUERY is used to query the database for documents in a collection.
  *
  * The database will respond to OP_QUERY messages with OP_REPLY.
  *
  * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPQUERY
  */
abstract class QueryMessage extends MongoClientMessage {
  // val header: MessageHeader // Standard message header
  val opCode = OpCode.OpQuery

  def flags: Int = {
    // bit vector of query options, assembled from QueryFlag
    var _f = 0
    if (tailableCursor) _f |= QueryFlag.TailableCursor.id
    if (slaveOK) _f |= QueryFlag.SlaveOk.id
    if (noCursorTimeout) _f |= QueryFlag.NoCursorTimeout.id
    if (awaitData) _f |= QueryFlag.AwaitData.id
    if (exhaustData) _f |= QueryFlag.Exhaust.id
    if (partialData) _f |= QueryFlag.Partial.id
    _f
  }

  val tailableCursor: Boolean
  val slaveOK: Boolean
  val noCursorTimeout: Boolean
  val awaitData: Boolean
  val exhaustData: Boolean
  val partialData: Boolean

  val namespace: String
  // Full collection name (dbname.collectionname)
  val numberToSkip: Int
  // number of documents to skip
  val numberToReturn: Int
  // number of docs to return in first OP_REPLY batch
  val query: BSONDocument
  // BSON Document representing the query
  val returnFields: Option[BSONDocument] // Optional BSON Document for fields to return


  /**
    * Message specific implementation.
    *
    * serializeHeader() writes the header, serializeMessage does a message
    * specific writeout
    */
  protected def serializeMessage()(implicit maxBSON: Int) = {
    val b = ByteString.newBuilder
    b.putInt(flags)
    ImmutableBSONDocumentComposer.composeCStringValue(namespace)(b)
    b.putInt(numberToSkip)
    b.putInt(numberToReturn)
    ImmutableBSONDocumentComposer.composeBSONObject(None /* field name */ , query.iterator)(b)
    returnFields.foreach { f =>
      ImmutableBSONDocumentComposer.composeBSONObject(None /* field name */ , f.iterator)(b)
    }
    b.result()
  }

  override def toString = "{ QueryMessage ns: %s, numSkip: %d, numReturn: %d, query: %s, fields: %s }".format(
    namespace, numberToSkip, numberToReturn, query, returnFields)
}

object QueryFlag extends Enumeration {
  val Reserved = Value(0) // Reserved value
  /**
    * Tailable cursor, which is not closed when last data is retrieved
    * Instead, the cursor marks the final object's position,
    * allowing resumption of the cursor later from where it was located
    * if more data is received.
    *
    * It is possible for the cursor to become invalid at some point
    * (CursorNotFound) such as if the final object it points at was
    * deleted.
    */
  val TailableCursor = Value(1 << 1)
  /**
    * Allow query from replica slaves.
    */
  val SlaveOk = Value(1 << 2)
  /**
    * Used internally for replication, driver should not use.
    */
  val OpLogReplay = Value(1 << 3)
  /**
    * Normally, the mongo server times out an idle cursor after an
    * inactivity period (~10 minutes) to prevent excess memory usage.
    * Setting this option prevents the cursor from timing out.
    */
  val NoCursorTimeout = Value(1 << 4)
  /**
    * Used in conjunction with TailableCursor.  If at the end of data,
    * AwaitData blocks rather than returning no data.
    * After a timeout period, returns as normal.
    * In an Async driver we may want to use this with some care.
    */
  val AwaitData = Value(1 << 5)
  /**
    * Stream the data down "full blast" across multiple "more" packages.
    * This assumes the client will fully read all data queried.
    * This can be much faster when you are pulling a lot of data
    * and known you want to pull it all down at once.
    *
    * TODO --- This is in our proto docs but unsure what it means? ---
    * NOTE: The client is *not* allowed to read all the data unless
    * it closes the connection.
    */
  val Exhaust = Value(1 << 6)
  /**
    * Get partial results from a mongos if some shards are down,
    * instead of the default behavior of throwing an error.
    */
  val Partial = Value(1 << 7)

  // Bits 8-31 are reserved for future usage
}

