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
import hammersmith.collection.immutable.Document
import hammersmith.bson.{BSONParser, SerializableBSONObject, ImmutableBSONDocumentParser}
import hammersmith.util.Logging
import akka.util.{ByteIterator, ByteString}

/**
 * OP_REPLY
 *
 * OP_REPLY is sent by the database in response to an OP_QUERY or
 * OP_GET_MORE message.
 *
 * TODO - Come back to this.  We need to figure out how to lay down the 'implicit' deserializer on decode.
 * TODO - Test the living fuck out of me, not INTEGRATION but actual Unit tests. N<atch.
 *
 *
 */
class ReplyMessage(val header: MessageHeader,
                   val flags: Int, // bit vector of reply flags, available in ReplyFlag
                   val cursorID: Long, // cursorID, for client to do getMores
                   val startingFrom: Int, // Where in the cursor this reply starts at
                   val numReturned: Int, // Number of documents in the reply.
                   val rawDocuments: ByteIterator // raw iterator of documents
                   ) extends MongoServerMessage {
  val opCode = OpCode.OpReply

  println("Flags ... " + flags)
  def cursorNotFound = (flags & ReplyFlag.CursorNotFound.id) > 0

  def queryFailure = (flags & ReplyFlag.QueryFailure.id) > 0

  def awaitCapable = (flags & ReplyFlag.AwaitCapable.id) > 0

  /**
   * My analysis shows that there's a definitive pause cycle when decoding all incoming documents
   * for a given batch *up front* - that is, you get a batch of $n documents and decode the BSON completely
   * before your operation such as a  cursor buffer replenishment proceeding.
   *
   * Instead, by breaking these down into a lazy stream we amortize costs:
   */
  def documents[T : SerializableBSONObject]: Stream[T] = {
    implicitly[SerializableBSONObject[T]].parser.asStream(numReturned, rawDocuments)
  }


  /**
   * Message specific implementation.
   *
   * serializeHeader() writes the header, serializeMessage does a message
   * specific writeout
   */
  protected def serializeMessage()(implicit maxBSON: Int) = ???

  override def toString = "ReplyMessage { " +
    "responseTo: %d, cursorID: %d, startingFrom: %d, numReturned: %d, cursorNotFound? %s, queryFailure? %s, awaitCapable? %s, # docs (reported): %d } ".
      format(header.responseTo, cursorID, startingFrom, numReturned, cursorNotFound, queryFailure, awaitCapable, numReturned)

}

object ReplyMessage extends Logging {


  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  /*
   * simple matcher to fetch requestID for protocol matching
   */
  def unapply(r: ReplyMessage): Option[Int] = Option(r.header.requestID)

  /**
   * New AkkaIO based decoder hierarchy for an incoming reply message.
   * @param hdr An instance of a wire protocol MessageHeader containing the core details of all messages
   * @param frame The ByteIterator representing the remainder of the network bytes for this reply following the ReplyMessage.
   * @return An instance ofa  ReplyMessage representing the incoming datastream
   */
  def apply(hdr: MessageHeader, frame: ByteIterator) = {
    println(s"Header: '$hdr', frame: '$frame', msg len: '${hdr.messageLength}, frame len: '${frame.len}'")
    val flags = frame.getInt
    val cursorID = frame.getLong
    val startingFrom = frame.getInt
    val numReturned = frame.getInt
    new ReplyMessage(hdr, flags, cursorID, startingFrom, numReturned, frame.clone())
  }

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

