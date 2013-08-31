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

import java.util.concurrent.atomic.AtomicInteger
import org.bson._
import java.io._
import org.bson.io.{ OutputBuffer, PoolOutputBuffer }
import hammersmith.bson.{ImmutableBSONDocumentParser, BSONSerializer, DefaultBSONSerializer}
import hammersmith.util.Logging
import akka.util.{ByteIterator, ByteString}


/**
 * Wire Protocol related code.
 *
 * @see http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol
 */

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
case class MessageHeader(
  /**
   * Total message size, in bytes
   * including the 4 bytes to hold this length
   */
  messageLength: Int,
  /**
   * The client or DB generated identifier which uniquely
   * identifies this message.
   *
   * For client generated messages (OpQuery, OpGetMore), this is
   * returned in the responseTo field for OpReply messages.
   *
   * This should be used to associate responses w/ originating queries.
   */
  requestID: Int,
  /**
   * For reply messages from the database, this contains the
   * requestId value from the original OpQuery/OpGetMore messages.
   *
   * It should be used to associate responses with the originating query.
   */
  responseTo: Int,
  /**
   * Request Type
   * @see OpCode
   */
  opCode: Int
)

object MongoMessage extends Logging {

  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  val ID = new AtomicInteger(1)

  /**
   * For servers incapable of specifying BSON Size (< 1.8), what's their max size?
   *  TODO - we aren't going to support these, at all.
   */
  val DefaultMaxBSONObjectSize = 1024 * 1024 * 4


  def apply(header: ByteString, frame: ByteString): MongoMessage =  apply(header.iterator, frame.iterator)

  /**
   * Extractor method for incoming streams of
   * MongoDB data.
   *
   * Attempts to decode them into a coherent message.
   *
   * For the moment can only decode Reply messages
   * longterm we'll support all messages for testing purposes.
   *
   */
  def apply(header: ByteIterator, frame: ByteIterator): MongoMessage = {
    log.debug("Attempting to extract a coherent MongoDB Message")


    val msgHeader = MessageHeader(
      messageLength = header.getInt,
      requestID = header.getInt,
      responseTo = header.getInt,
      opCode = header.getInt
    )

    log.debug(s"Message Header decoded '$msgHeader'")


    OpCode(msgHeader.opCode) match {
      case OpCode.OpReply ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_REPLY'")
        ReplyMessage(msgHeader, frame)
      }
      case OpCode.OpMsg ⇒ {
        log.warn("[Incoming Message] Deprecated message type 'OP_MSG' received.")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpUpdate ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_UPDATE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpInsert ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_INSERT'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpGetMore ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_GET_MORE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpDelete ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_DELETE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpKillCursors ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_KILL_CURSORS'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case unknown ⇒ {
        log.error("Unknown Message OpCode '%d'", unknown)
        throw new UnsupportedOperationException("Invalid Message Type with OpCode '%d'".format(unknown))
      }
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
   *
   * @deprecated This is the old methods for Netty.
   * TODO - DELETE ME
   */
  def unapply(in: InputStream): MongoMessage = {
    import org.bson.io.Bits._
    log.debug("Attempting to extract a coherent MongoDB Message from '%s'", in)
    val b = new Array[Byte](16) // Message header
    log.trace("Native HDR Allocated to %d", b.length)
    readFully(in, b)
    val rawHdr = new ByteArrayInputStream(b)
    log.trace("Message Header: %s", rawHdr.toString)

    val header = MessageHeader(
      messageLength = readInt(rawHdr),
      requestID = readInt(rawHdr),
      responseTo = readInt(rawHdr),
      opCode = readInt(rawHdr)
    )

    OpCode(header.opCode) match {
      case OpCode.OpReply ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_REPLY'")
        ReplyMessage(header, in)
      }
      case OpCode.OpMsg ⇒ {
        log.warn("[Incoming Message] Deprecated message type 'OP_MSG' received.")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpUpdate ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_UPDATE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpInsert ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_INSERT'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpGetMore ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_GET_MORE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpDelete ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_DELETE'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case OpCode.OpKillCursors ⇒ {
        log.debug("[Incoming Message] OpCode is 'OP_KILL_CURSORS'")
        throw new UnsupportedOperationException("Unsupported operation type for reads.")
      }
      case unknown ⇒ {
        log.error("Unknown Message OpCode '%d'", unknown)
        throw new UnsupportedOperationException("Invalid Message Type with OpCode '%d'".format(unknown))
      }
    }
  }
}

abstract class MongoMessage extends Logging {

  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
  /* Standard Message Header */
  //val header: MessageHeader
  val opCode: OpCode.Value
  val requestID = MongoMessage.ID.getAndIncrement
  log.trace("Generated Message ID '%s'", requestID)

  def serialize()(implicit maxBSON: Int): ByteString = {
    val head = serializeHeader()
    val tail = serializeMessage()
    val msg = head ++ tail
    val b = ByteString.newBuilder
    b.putInt(msg.length)
    val len = b.result()
    // todo - should we compact this????
    len ++ msg
  }

  /**
   * Serialize the message header.
   * @param maxBSON
   * @return
   */
  protected def serializeHeader()(implicit maxBSON: Int): ByteString = {
    throw new UnsupportedOperationException("BSON Composition not yet supported.")
  }

  /*
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
  */
  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int): ByteString
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
  def ids: Seq[Option[Any]] // All IDs  this message is writing... used for callback fun
}

/**
 * A message sent from a mongodb server to a client
 */
abstract class MongoServerMessage extends MongoMessage

