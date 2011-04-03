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

import com.mongodb.util.Logging
import java.nio.ByteOrder
import java.util.concurrent.atomic.AtomicInteger
import org.bson.io.PoolOutputBuffer
import org.bson.{ BSONEncoder, BSONObject }
import org.jboss.netty.buffer.{ ChannelBufferOutputStream, ChannelBuffers }
import org.jboss.netty.channel.Channel

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

/**
 * TODO - Buffer Pooling like PoolOutputBuffer (which is package fucked right now and can't be subclassed for this)
 */
abstract class MongoMessage extends Logging {
  /* Standard Message Header */
  //val header: MessageHeader
  val opCode: OpCode.Value
  var requestID = -1

  //  def apply(channel: Channel) = write

  // TODO - Decouple me... this is bad design
  def write(out: Channel) = {
    val enc = new BSONEncoder
    // TODO - Better pre-estimation of buffer size
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 1024 * 1024 * 4))
    val buf = new PoolOutputBuffer()
    enc.set(buf)

    enc.writeInt(0) // Length, will set later; for now, placehold

    val id = MongoMessage.ID.getAndIncrement()
    log.trace("Generated Message ID '%s'", id)

    enc.writeInt(id)
    enc.writeInt(0) // Response ID left empty
    enc.writeInt(opCode.id) // opCode Type
    log.trace("OpCode (%s) Int Type: %s", opCode, opCode.id)

    writeMessage(enc)
    log.trace("Finishing writing core message, final length of '%s'", buf.size)
    buf.write(new Array(buf.size.toByte), 0, 4)
    buf.pipe(outStream)
    out.write(outStream)
  }

  protected def writeMessage(enc: BSONEncoder)
}

object UpdateFlag extends Enumeration {
  val Upsert = Value(1 << 0)
  val MultiUpdate = Value(1 << 1)
  // Bits 2-31 are reserved for future usage
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

  val query: BSONDocument // The query document to select from mongo
  val update: BSONDocument // The document specifying the update to perform

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(flags)
    // TODO - Check against Max BSON Size
    enc.putObject(query)
    enc.putObject(update)
  }
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
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpInsert

  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val documents: Seq[BSONDocument] // One or more documents to insert into the collection

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    // TODO - Limit batch insert size which should be 4 * MaxBSON
    for (doc <- documents) enc.putObject(doc)

  }
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
  // val header: MessageHeader // Standard message header
  val opCode = OpCode.OpQuery

  def flags: Int = { // bit vector of query options, assembled from QueryFlag
    var _f = 0
    if (tailableCursor) _f |= QueryFlag.TailableCursor.id
    if (slaveOk) _f |= QueryFlag.SlaveOk.id
    if (noCursorTimeout) _f |= QueryFlag.NoCursorTimeout.id
    if (awaitData) _f |= QueryFlag.AwaitData.id
    if (exhaust) _f |= QueryFlag.Exhaust.id
    if (partial) _f |= QueryFlag.Partial.id
    _f
  }

  val tailableCursor: Boolean
  val slaveOk: Boolean
  val noCursorTimeout: Boolean
  val awaitData: Boolean
  val exhaust: Boolean
  val partial: Boolean

  val namespace: String // Full collection name (dbname.collectionname)
  val numberToSkip: Int // number of documents to skip
  val numberToReturn: Int // number of docs to return in first OP_REPLY batch
  val query: BSONDocument // BSON Document representing the query
  val returnFields: Option[BSONDocument] = None // Optional BSON Document for fields to return

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(flags)
    enc.writeCString(namespace)
    enc.writeInt(numberToSkip)
    enc.writeInt(numberToReturn)
    enc.putObject(query)
    // TDOO - Not sure what to write for None as this is optional
    enc.putObject(returnFields.getOrElse(null))
  }
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
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpGetMore
  val ZERO: Int = 0 // 0 - reserved for future use
  val namespace: String // Full collection name (dbname.collectionname)
  val numberToReturn: Int // number of docs to return in first OP_REPLY batch
  val cursorID: Long // CursorID from the OP_REPLY (DB Genned value)

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(numberToReturn)
    enc.writeLong(cursorID)
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

  val query: BSONDocument // Query object for what to delete

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(ZERO)
    enc.writeCString(namespace)
    enc.writeInt(flags)
    enc.putObject(query)
  }
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
  // val header: MessageHeader // Standard message header
  val opCode = OpCode.OpKillCursors
  val ZERO: Int = 0 // 0 - reserved for future use
  val numCursors: Int // The number of cursorIDs in the message
  val cursorIDs: Seq[Long] // Sequence of cursorIDs to close

  protected def writeMessage(enc: BSONEncoder) {
    enc.writeInt(ZERO)
    enc.writeInt(numCursors)
    for (_id <- cursorIDs) enc.writeLong(_id)
  }
}

/** OP_MSG Deprecated and not implemented */

object ReplyFlag extends Enumeration {
  /**
   * Set when getMore is called but the cursorID is not valid
   * on the server.  Returns with 0 results.
   */
  val CursorNotFound = Value(1 << 0)

  /**
   * Set when a query fails.  Results consist of one document
   * containing an "$err" field describing the failure.
   */
  val QueryFailure = Value(1 << 1)

  /**
   * Drivers should ignore this; only mongos ever sees it set,
   * in which case it needs to update config from the server
   */
  val ShardConfigState = Value(1 << 2)

  /**
   * Set when the server supports the AwaitData Query Option.  If
   * it doesn't, a client should sleep a little between getMore's on
   * a Tailable cursor.  MongoDB 1.6+ support AwaitData and thus always
   * sets AwaitCapable
   */
  val AwaitCapable = Value(1 << 3)

  // Bits 4-31 are reserved for future usage
}
/**
 * OP_REPLY
 *
 * OP_REPLY is sent by the database in response to an OP_QUERY or
 * OP_GET_MORE message.
 *
 */
trait ReplyMessage extends MongoMessage {
  //val header: MessageHeader // Standard message header
  val opCode = OpCode.OpReply
  // TODO - Read flags in!
  val flags: Int // bit vector of reply flags, available in ReplyFlag
  def cursorNotFound = flags & ReplyFlag.CursorNotFound.id
  def queryFailure = flags & ReplyFlag.QueryFailure.id
  def awaitCapable = flags & ReplyFlag.AwaitCapable.id
  val cursorID: Long // cursorID, for client to do getMores
  val startingFrom: Int // Where in the cursor this reply starts at
  val numReturned: Int // Number of documents in the reply.
  val documents: Seq[BSONDocument] // Sequence of documents

  protected def writeMessage(enc: BSONEncoder) =
    throw new UnsupportedOperationException("This message is not capable of being written. "
      + "Replies come only from the server.")
}
