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

package com.mongodb
package wire

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
import java.io.{ EOFException, InputStream, OutputStream }

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
    val rawHdr = Array[Byte](16) // Message header
    readFully(in, rawHdr)
    log.trace("Message Header: %s", rawHdr)

    val header = new MessageHeader {
      val messageLength = readInt(rawHdr)
      log.trace("Message Length: %i", messageLength)
      // TODO - Validate message length
      val requestID = readInt(rawHdr)
      log.trace("Message ID: %i", requestID)
      val responseTo = readInt(rawHdr)
      log.trace("Message Response To (ID): %i", responseTo)
      val opCode = readInt(rawHdr)
      log.trace("Operation Code: %i", opCode)
    }

    header.opCode match {
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
        log.error("Unkown Message OpCode '%i'", unknown)
        throw new UnsupportedOperationException("Invalid Message Type with OpCode '%d'".format(unknown))
      }
    }
  }
}

abstract class MongoMessage extends Logging {
  /* Standard Message Header */
  //val header: MessageHeader
  val opCode: OpCode.Value
  var requestID = -1

  //  def apply(channel: Channel) = write

  // TODO - Decouple me... this is bad design
  def write(out: OutputStream) = {
    val enc = new BSONEncoder

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
    buf.pipe(out)
  }

  /**
   * Message specific implementation.
   *
   * write() puts in the header, writeMessage does a message
   * specific writeout
   */
  protected def writeMessage(enc: BSONEncoder)
}

