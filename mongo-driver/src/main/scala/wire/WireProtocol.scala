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

/**
 * Wire Protocol related code.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
 */

import java.util.concurrent.atomic.AtomicInteger
import org.bson.BSONObject

/**
 * Request OpCodes for communicating with MongoDB Servers
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-RequestOpcodes
 */
object OpCode extends Enumeration {
  val OpReply = Value(1)
  val OpMsg = Value(1000)
  val OpUpdate = Value(2001)
  val OpInsert = Value(2002)
  val Reserved = Value(2003)
  val OpQuery = Value(2004)
  val OpGetMore = Value(2005)
  val OpDelete = Value(2006)
  val OpKillCursors = Value(2007)
}

/**
 * Standard Message header for Mongo communication
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-StandardMessageHeader
 */
trait MessageHeader {
  /**
   * Total message size, in bytes
   * including the 4 bytes to hold this length
   */
  val messageLength: Int

  /**
   * The client or DB generated identifier which uniquely
   * identifies this message.
   *
   * For client generated messages (OpQuery, OpGetMore), this is
   * returned in the responseTo field for OpReply messages.
   *
   * This should be used to associate responses w/ originating queries.
   */
  val requestID: Int

  /**
   * For reply messages from the database, this contains the
   * requestId value from the original OpQuery/OpGetMore messages.
   *
   * It should be used to associate responses with the originating query.
   */
  val responseTo: Int

  /**
   * Request Type
   * @see OpCode
   */
  val opCode: Int
}

/* Placeholder for future usage
 * TODO Implement me
 */
trait BSONDocument extends BSONObject

object MongoMessage {
  val ID = new AtomicInteger(1)
}

abstract class MongoMessage {
  /** Standard Message Header */
  val header: MessageHeader

}

object UpdateFlag extends Enumeration {
  val Upsert = Value(0)
  val MultiUpdate = Value(1)
  // 2-31 are reserved for future usage
}

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
  val header: MessageHeader // standard message header
  val ZERO: Int // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val flags: Int // bit vector of UpdateFlags assembled from UpdateFlag
  val query: BSONDocument // The query document to select from mongo
  val update: BSONDocument // The document specifying the update to perform
}

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
trait InsertMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val ZERO: Int // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val documents: Seq[BSONDocument] // One or more documents to insert into the collection
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
  val TailableCursor = Value(1)
  /**
   * Allow query from replica slaves.
   */
  val SlaveOk = Value(2)
  /**
   * Used internally for replication, driver should not use.
   */
  val OpLogReplay = Value(3)
  /**
   * Normally, the mongo server times out an idle cursor after an
   * inactivity period (~10 minutes) to prevent excess memory usage.
   * Setting this option prevents the cursor from timing out.
   */
  val NoCursorTimeout = Value(4)
  /**
   * Used in conjunction with TailableCursor.  If at the end of data,
   * AwaitData blocks rather than returning no data.
   * After a timeout period, returns as normal.
   * In an Async driver we may want to use this with some care.
   */
  val AwaitData = Value(5)
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
  val Exhaust = Value(6)
  /**
   * Get partial results from a mongos if some shards are down,
   * instead of the default behavior of throwing an error.
   */
  val Partial = Value(7)

  // 8-31 are reserved for future usage
}
/**
 * OP_QUERY Message
 *
 * OP_QUERY is used to query the database for documents in a collection.
 *
 * The database will respond to OP_QUERY messages with OP_REPLY.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPQUERY
 */
trait QueryMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val flags: Int // bit vector of query options, assembled from QueryFlag
  val namespace: String // Full collection name (dbname.collectionname)
  val numberToSkip: Int // number of documents to skip
  val numberToReturn: Int // number of docs to return in first OP_REPLY batch
  val query: BSONDocument // BSON Document representing the query
  val returnFields: Option[BSONDocument] = None // Optional BSON Document for fields to return
}

/**
 * OP_GET_MORE Message
 *
 * OP_GET_MORE is used to query the database for documents in a collection.
 *
 * The database will respond to OP_GET_MORE messages with OP_REPLY.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPGETMORE
 */
trait GetMoreMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val ZERO: Int // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val numberToReturn: Int // number of docs to return in first OP_REPLY batch
  val cursorID: Long // CursorID from the OP_REPLY (DB Genned value)
}

object DeleteFlag extends Enumeration {
  /**
   * If set, remove only first matching doc.
   * Default behavior removes all matching documents.
   */
  val SingleRemove = 0

  // 1-31 are reserved for future usage
}

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
trait DeleteMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val ZERO: Int // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val flags: Int // bit vector of delete flags assembled from DeleteFlag
  val query: BSONDocument // Query object for what to delete
}

/**
 * OP_KILL_CURSORS
 *
 * OP_KILL_CURSORS is used to close an active cursor in the database,
 * to allow resources on the dbserver to be reclaimed after a query.
 *
 * Note that if a cursor is read until exhausted (read until OP_QUERY or OP_GET_MORE returns zero for the cursor id),
 * there is no need to kill the cursor.
 */
trait KillCursorsMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val ZERO: Int // 0 - reserved for future use
  val numCursors: Int // The number of cursorIDs in the message
  val cursorIDs: Seq[Long] // Sequence of cursorIDs to close
}

/** OP_MSG Deprecated and not implemented */

object ReplyFlag extends Enumeration {
  /**
   * Set when getMore is called but the cursorID is not valid
   * on the server.  Returns with 0 results.
   */
  val CursorNotFound = Value(0)

  /**
   * Set when a query fails.  Results consist of one document
   * containing an "$err" field describing the failure.
   */
  val QueryFailure = Value(1)

  /**
   * Drivers should ignore this; only mongos ever sees it set,
   * in which case it needs to update config from the server
   */
  val ShardConfigState = Value(2)

  /**
   * Set when the server supports the AwaitData Query Option.  If
   * it doesn't, a client should sleep a little between getMore's on
   * a Tailable cursor.  MongoDB 1.6+ support AwaitData and thus always
   * sets AwaitCapable
   */
  val AwaitCapable = Value(3)

  // 4-31 are reserved for future usage
}
/**
 * OP_REPLY
 *
 * OP_REPLY is sent by the database in response to an OP_QUERY or
 * OP_GET_MORE message.
 *
 */
trait ReplyMessage extends MongoMessage {
  val header: MessageHeader // Standard message header
  val flags: Int // bit vector of reply flags, available in ReplyFlag
  val cursorID: Long // cursorID, for client to do getMores
  val startingFrom: Int // Where in the cursor this reply starts at
  val numReturned: Int // Number of documents in the reply.
  val documents: Seq[BSONDocument] // Sequence of documents
}
