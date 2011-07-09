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

/**
 * Wire Protocol related code.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
 */

import org.bson.util.Logging
import java.util.concurrent.atomic.AtomicInteger
import org.bson._
import java.io._
import org.bson.io.{ OutputBuffer, PoolOutputBuffer }

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

object MongoMessage extends Logging {
  val ID = new AtomicInteger(1)

  /**
   * For servers incapable of specifying BSON Size (< 1.8), what's their max size?
   */
  val DefaultMaxBSONObjectSize = 1024 * 1024 * 4

  def readFromOffset(in: InputStream, b: Array[Byte], offset: Int) {
    readFromOffset(in, b, offset, b.length)
  }

  def readFromOffset(in: InputStream, b: Array[Byte], offset: Int, len: Int) {
    var x = offset
    while (x < len) {
      val n = in.read(b, x, len - x)
      if (n < 0) throw new EOFException
      x += n
    }
  }

  /**
   * Extractor method for incoming streams of
   * MongoDB data.
   *
   * Attempts to decode them into a coherent message.
   *
   * For the moment can only decode Reply messages
   * longterm we'll support all messages for testing purposes.
   */
  def unapply(in: InputStream): MongoMessage = {
    import org.bson.io.Bits._
    log.debug("Attempting to extract a coherent MongoDB Message from '%s'", in)
    val b = new Array[Byte](16) // Message header
    log.trace("Raw HDR Allocated to %d", b.length)
    readFully(in, b)
    val rawHdr = new ByteArrayInputStream(b)
    log.trace("Message Header: %s", rawHdr.toString)

    val header = new MessageHeader {
      val messageLength: Int = readInt(rawHdr)
      log.trace("Message Length: %d", messageLength)
      // TODO - Validate message length
      val requestID = readInt(rawHdr)
      log.trace("Message ID: %d", requestID)
      val responseTo = readInt(rawHdr)
      log.trace("Message Response To (ID): %d", responseTo)
      val opCode = readInt(rawHdr)
      log.trace("Operation Code: %d", opCode)
    }

    OpCode(header.opCode) match {
      case OpCode.OpReply => {
        log.debug("[Incoming Message] OpCode is 'OP_REPLY'")
        ReplyMessage(header, in)
      }
      case OpCode.OpMsg => {
        log.warn("[Incoming Message] Deprecated message type 'OP_MSG' received.")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpUpdate => {
        log.debug("[Incoming Message] OpCode is 'OP_UPDATE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpInsert => {
        log.debug("[Incoming Message] OpCode is 'OP_INSERT'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpGetMore => {
        log.debug("[Incoming Message] OpCode is 'OP_GET_MORE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpDelete => {
        log.debug("[Incoming Message] OpCode is 'OP_DELETE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpKillCursors => {
        log.debug("[Incoming Message] OpCode is 'OP_KILL_CURSORS'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case unknown => {
        log.error("Unknown Message OpCode '%d'", unknown)
        throw new UnsupportedOperationException("Invalid Message Type with OpCode '%d'".format(unknown))
      }
    }
  }
}

abstract class MongoMessage extends Logging {
  /* Standard Message Header */
  //val header: MessageHeader
  val opCode: OpCode.Value
  var requestID = MongoMessage.ID.getAndIncrement
  log.trace("Generated Message ID '%s'", requestID)

  //  def apply(channel: Channel) = write

  def write(out: OutputStream)(implicit maxBSON: Int) = {
    // TODO - Reuse / pool Serializers for performance via reset()
    val buf = new PoolOutputBuffer()
    val enc = new DefaultBSONSerializer
    enc.set(buf)
    val sizePos = buf.getPosition
    build(enc)
    log.trace("Finishing writing core message, final length of '%s'", buf.size)
    buf.writeInt(sizePos, buf.getPosition - sizePos)
    buf.pipe(out)
  }

  protected def build(enc: BSONSerializer)(implicit maxBSON: Int) = {
    enc.writeInt(0) // Length, will set later; for now, placehold

    enc.writeInt(requestID)
    enc.writeInt(0) // Response ID left empty
    enc.writeInt(opCode.id) // opCode Type
    log.trace("OpCode (%s) Int Type: %s", opCode, opCode.id)

    writeMessage(enc)
  }

  /**
   * Message specific implementation.
   *
   * write() puts in the header, writeMessage does a message
   * specific writeout
   */
  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int)
}

/**
 * A message sent from a client to a mongodb server
 */
abstract class MongoClientMessage extends MongoMessage

/**
 * Any client -> server message which writes
 */
abstract class MongoClientWriteMessage extends MongoClientMessage {
  val namespace: String // Full collection name (dbname.collectionname)
  def ids: Seq[Option[AnyRef]] // All IDs  this message is writing... used for callback fun
}

/**
 * A message sent from a mongodb server to a client
 */
abstract class MongoServerMessage extends MongoMessage

