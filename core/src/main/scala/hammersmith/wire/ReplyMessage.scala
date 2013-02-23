/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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

package hammersmith
package wire

import java.io.{ByteArrayInputStream, InputStream}
import org.bson._
import hammersmith.collection.immutable.Document
import hammersmith.bson.{DefaultBSONParser, BSONSerializer, DefaultBSONDeserializer}
import hammersmith.util.Logging
import akka.util.{ByteIterator, ByteString}

/**
 * OP_REPLY
 *
 * OP_REPLY is sent by the database in response to an OP_QUERY or
 * OP_GET_MORE message.
 *
 * TODO - Come back to this.  We need to figure out how to lay down the 'implicit' deserializer on decode.
 *
 */
class ReplyMessage(val header: MessageHeader,
                   val flags: Int, // bit vector of reply flags, available in ReplyFlag
                   val cursorID: Long, // cursorID, for client to do getMores
                   val startingFrom: Int, // Where in the cursor this reply starts at
                   val numReturned: Int, // Number of documents in the reply.
                   val documents: Stream[Document] // Sequence of documents TODO Definable T
                   ) extends MongoServerMessage {
  val opCode = OpCode.OpReply

  def cursorNotFound = (flags & ReplyFlag.CursorNotFound.id) > 0

  def queryFailure = (flags & ReplyFlag.QueryFailure.id) > 0

  def awaitCapable = (flags & ReplyFlag.AwaitCapable.id) > 0

  protected def writeMessage(enc: BSONSerializer)(implicit maxBSON: Int) =
    throw new UnsupportedOperationException("This message is not capable of being written. "
      + "Replies come only from the server.")

  /*
   * And here comes the hairy part.  Ideally, we want to completely amortize the
   * decoding of these docs.  It makes *zero* sense to me to wait for a whole
   * block of documents to decode from BSON before I can begin iteration.
   */
  assert(documents.length == numReturned, "Number of parsed documents doesn't match expected number returned." +
    "Wanted: %d Got: %d".format(numReturned, documents.length))
  log.trace("Parsed Out '%d' Documents", documents.length)

  override def toString = "ReplyMessage { " +
    "responseTo: %d, cursorID: %d, startingFrom: %d, numReturned: %d, cursorNotFound? %s, queryFailure? %s, awaitCapable? %s, # docs: %d } ".
      format(header.responseTo, cursorID, startingFrom, numReturned, cursorNotFound, queryFailure, awaitCapable, documents.length)

}

object ReplyMessage extends Logging {


  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  def apply(_hdr: MessageHeader, frame: ByteIterator) = {
    // TODO - Make it possible to dynamically set a decoder.
    // _hdr is the generic 'every protocol message has it' header; another 20 bytes of reply header data
    val b = frame.clone().take(20)
    frame.drop(20)

    log.trace("Offset data for rest of reply read: %s", b)
    val flags = b.getInt
    val cursorID = b.getLong
    val startingFrom = b.getInt
    val numReturned = b.getInt
    new ReplyMessage(_hdr, flags, cursorID, startingFrom, numReturned,
                    documents = DefaultBSONParser.asStream(numReturned, frame))
  }

  /** @deprecated this is the old netty era decoder. */
  def apply(_hdr: MessageHeader, in: InputStream) = {
    import org.bson.io.Bits._
    // TODO - Make it possible to dynamically set a decoder.
    /**
     * TODO - It turned out to NOT be safe to share this directly and we'll need a pool.
     */
    val decoder = new DefaultBSONDeserializer
    log.debug("Finishing decoding Reply Message with Header of '%s'", _hdr)
    val b = new Array[Byte](20) // relevant non-document stream bytes from the reply content.
    readFully(in, b)
    val bin = new ByteArrayInputStream(b)
    log.trace("Offset data for rest of reply read: %s", bin)

    val flags = readInt(bin)
    val cursorID = readLong(bin)
    val startingFrom = readInt(bin)
    val numReturned = readInt(bin)
    /*
     * And here comes the hairy part.  Ideally, we want to completely amortize the
     * decoding of these docs.  It makes *zero* sense to me to wait for a whole
     *  block of documents to decode from BSON before I can begin iteration.
     **/
    import org.bson.io.Bits
    def _dec() = {
      val l = Array.ofDim[Byte](4)
      in.read(l)
      val len = Bits.readInt(l)
      log.debug("Decoding object, length: %d", len)
      val b = Array.ofDim[Byte](len)
      in.read(b, 4, len - 4)
      // copy length to the full array
      Array.copy(l, 0, b, 0, 4)
      log.trace("Len: %s L: %s / %s, Header: %s", len, l, readInt(l), readInt(b))
      DefaultBSONParser.apply(ByteString(b).iterator)
    }

    val documents = for (i ← (0 until numReturned).toStream) yield _dec

    new ReplyMessage(_hdr, flags, cursorID, startingFrom, numReturned, documents)
  }
}

class ReplyDocuments(numReturned: Int, docStream: ByteIterator) extends Stream[Document] {
  override def isEmpty = numReturned <= 0

  override def head = throw new NoSuchElementException("head of empty stream")

  override def tail = throw new UnsupportedOperationException("tail of empty stream")

  def tailDefined = false
}

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

