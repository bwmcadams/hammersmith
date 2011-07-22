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

import akka.actor.{ Channel => AkkaChannel, _ }
import com.mongodb.async.wire._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.bson.util.Logging
import java.nio.ByteOrder

/**
 * A DirectConnectionActor is a ConnectionActor wrapping a single netty channel.
 * These then go in an actor pool. Maybe should not be called DirectConnectionActor,
 * something like SingleChannelActor perhaps?
 */
private[mongodb] class DirectConnectionActor(private val channel: Channel,
  private implicit val maxBSONObjectSize: Int)
  extends ConnectionActor
  with Actor
  with Logging {
  import ConnectionActor._

  private case class ClientSender(channel: AkkaChannel[Any], outgoingReplyBuilder: (ReplyMessage) => Outgoing)

  // remember, no need for any of this to be thread-safe since
  // actor runs in only one thread at a time.
  private var senders = Map[Int, ClientSender]()

  override def receive = {
    case incoming: Incoming => incoming match {
      // message is from the app
      case clientWriteMessage: SendClientMessage => {
        sendMessageToMongo(self.channel, clientWriteMessage)
      }
      // message is from the netty channel handler
      case ServerMessageReceived(message) => {
        message match {
          case reply: ReplyMessage =>
            handleReplyMessage(reply)
        }
      }
      // message is from the netty channel handler
      case ChannelError(channelDescription, exception) => {
        val failMessage = ConnectionFailure(exception)
        failAllPending(failMessage)
      }
      // message is from the netty channel handler
      case ChannelClosed(channelDescription) => {
        val failMessage = ConnectionFailure(new Exception("Channel %s is closed".format(channelDescription)))
        failAllPending(failMessage)
      }
    }
  }

  private def failAllPending(failMessage: ConnectionFailure) = {
    val oldSenders = senders
    senders = Map()
    oldSenders foreach { kv =>
      kv._2.channel ! failMessage
    }
  }

  private def handleReplyMessage(message: ReplyMessage) = {
    senders.get(message.header.responseTo) foreach { client =>
      senders = senders - message.header.responseTo
      client.channel ! client.outgoingReplyBuilder(message)
    }
  }

  private def sendMessageToMongo(senderChannel: AkkaChannel[Any], clientRequest: SendClientMessage) = {
    val maybeReplyBuilder = clientRequest match {
      case r: SendClientGetMoreMessage =>
        Some(ConnectionActor.buildGetMoreReply(_))
      case r: SendClientCursorMessage =>
        Some({ reply: ReplyMessage =>
          ConnectionActor.buildCursorReply(self, r.message.namespace, reply)
        })
      case r: SendClientSingleDocumentMessage =>
        Some(ConnectionActor.buildSingleDocumentReply(_))
      case r: SendClientOptionalSingleDocumentMessage =>
        Some(ConnectionActor.buildOptionalSingleDocumentReply(_))
      case r: SendClientKillCursorsMessage =>
        None // fire and forget
      case r: SendClientSingleWriteMessage => // FIXME have to handle need for GLE
        Some(ConnectionActor.buildWriteReply(_))
      case r: SendClientBatchWriteMessage => // FIXME have to handle need for GLE
        Some(ConnectionActor.buildBatchWriteReply(_))
    }
    maybeReplyBuilder foreach { builder =>
      senders = senders + Pair(clientRequest.message.requestID, ClientSender(senderChannel, builder))
    }
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))
    log.trace("PreWrite with outStream '%s'", outStream)

    clientRequest.message.write(outStream)
    log.debug("Writing Message '%s' out to Channel via stream '%s'.", clientRequest.message, outStream)

    /* FIXME all the below logic has to be ported over from MongoConnection */
    /*
    require(channel.isConnected, "Channel is closed.")
    val isWrite = f.isInstanceOf[WriteRequestFuture]
    // TODO - Better pre-estimation of buffer size - We don't have a length attributed to the Message yet
    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))
    log.trace("Put msg id: %s f: %s into dispatcher: %s", msg.requestID, f, dispatcher)
    log.trace("PreWrite with outStream '%s'", outStream)
    /**
     * We only setup dispatchers if it is a Non-Write Request or a Write Request w/ a Write Concern that necessitates GLE
     * The GLE / Safe write stuff is setup later
     */
    if (!isWrite) dispatcher.put(msg.requestID, CompletableRequest(msg, f))
    msg.write(outStream)
    log.debug("Writing Message '%s' out to Channel via stream '%s'.", msg, outStream)

    /**
     * Determine if we need to execute a GetLastError (E.G. w > 0),
     * or execute a non-GLEed immediate callback against write requests.
     */
    // Quick callback when needed to be invoked immediately after write
    val writeCB: () => Unit = if (isWrite) {
      msg match {
        case wMsg: MongoClientWriteMessage => if (concern.safe_?) {
          val gle = createCommand(wMsg.namespace.split("\\.")(0), Document("getlasterror" -> 1))
          log.trace("Created a GetLastError Message: %s", gle)
          /**
           * We only setup dispatchers if it is a Non-Write Request or a Write Request w/ a Write Concern that necessitates GLE
           * Note we dispatch the GetLastError's ID but with the write message !
           */
          dispatcher.put(gle.requestID, CompletableRequest(msg, f))
          gle.write(outStream)
          log.debug("Wrote a getLastError to the tail end of the output buffer.")
          () => {}
        } else () => { wMsg.ids.foreach(x => f((x, WriteResult(true)).asInstanceOf[f.T])) }
        case unknown => {
          val e = new IllegalArgumentException("Invalid type of message passed; WriteRequestFutures expect a MongoClientWriteMessage underneath them. Got " + unknown)
          log.error(e, "Error in write.")
          () => { f(e) }
        }
      }
    } else () => {}
    // todo - clean this up to be more automatic like the writeCB is

    val exec = (_maxBSON: Int) => {
      channel.write(outStream.buffer())
      outStream.close()
      /** If no write Concern and it's a write, kick the callback now.*/
      writeCB()
    }
    // If the channel is open, it still doesn't mean we have a valid Mongo Connection.
    if (!channelState(channel).get && !_overrideLiveCheck) {
      log.info("Channel is not currently considered 'live' for MongoDB... May still be connecting or recovering from a Replica Set failover. Queueing operation. (override? %s) ", _overrideLiveCheck)
      channelOpQueue.getOrElseUpdate(channel, new ConcurrentQueue) += exec
    } else exec(maxBSONObjectSize)
*/
  }
}
