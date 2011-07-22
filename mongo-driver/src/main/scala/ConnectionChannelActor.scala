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
import com.mongodb.async.util._
import org.bson._
import org.bson.collection._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.bson.util.Logging
import java.nio.ByteOrder
import java.net.InetSocketAddress
import java.util.concurrent._

/**
 * A ConnectionChannelActor is a ConnectionActor wrapping a single netty channel.
 * These then go in an actor pool.
 */
private[mongodb] class ConnectionChannelActor(private val addr: InetSocketAddress)
  extends ConnectionActor
  with Actor {
  import ConnectionActor._
  import ConnectionChannelActor._

  log.trace("Constructing ConnectionChannelActor")

  private case class ClientSender(channel: AkkaChannel[Any], outgoingReplyBuilder: (ReplyMessage) => Outgoing)

  // remember, no need for any of this to be thread-safe since
  // actor runs in only one thread at a time.
  private var senders = Map[Int, ClientSender]()

  // channel and max BSON size are created asynchronously
  private var maybeChannel: Option[Channel] = None
  private implicit var maxBSONObjectSize = MongoMessage.DefaultMaxBSONObjectSize
  private var isMaster = false

  private val addressString = addr.toString

  private def startOpeningChannel() = {
    // don't get any messages until we get our channel open
    self.dispatcher.suspend(self)
    log.trace("Suspended message delivery to %s", self.uuid)

    val bootstrap = new ClientBootstrap(channelFactory)
    val pipelineFactory = new ConnectionActorPipelineFactory(self, addressString)

    bootstrap.setPipelineFactory(pipelineFactory)

    bootstrap.setOption("remoteAddress", addr)
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)
    /* AdaptiveReceiveBufferSizePredictor gradually scales the buffer up and down
     * depending on how many bytes arrive in each read()
     */
    bootstrap.setOption("child.receiveBufferSizePredictor",
      new AdaptiveReceiveBufferSizePredictor(128, /* minimum */
        256, /* initial */
        1024 * 1024 * 4 /* max */ ));

    val futureChannel = bootstrap.connect()

    futureChannel.addListener(new ChannelFutureListener() {
      val connectionActor = self
      // CAUTION we are coming in to the actor from an outside
      // thread here; the safety is that we keep the channel suspended
      // so should not get messages or do anything else with it until
      // this completes.
      override def operationComplete(f: ChannelFuture) = {
        log.trace("ChannelFutureListener notified for %s", connectionActor.uuid)
        if (f.isSuccess) {
          log.debug("Successfully opened a new channel %s", addressString)
          maybeChannel = Some(f.getChannel)

          // And yet another thread, but again this thread should be the only one
          // touching the fields in our actor until it completes, since we're
          // still suspended.
          val t = new Thread(new Runnable() {
            override def run = {
              log.trace("Waiting on setup steps to complete for actor %s", connectionActor.uuid)
              pipelineFactory.awaitSetup()
              log.trace("Setup steps completed for actor %s", connectionActor.uuid)
              if (pipelineFactory.setupFailed) {
                log.error("Failed to setup %s, suiciding actor: %s", addressString, pipelineFactory.setupFailure.getMessage)
                maybeChannel.get.close()
                maybeChannel = None
                connectionActor.stop()
              } else {
                maxBSONObjectSize = pipelineFactory.maxBSONObjectSize
                isMaster = pipelineFactory.isMaster

                log.debug("Resuming %s %s with max size %d and isMaster %s",
                  addressString, connectionActor.uuid, maxBSONObjectSize, isMaster)

                // now we can get messages
                connectionActor.dispatcher.resume(connectionActor)
              }
              log.trace("Setup thread exiting for %s", self.uuid)
            }
          },
            "Setup Hammersmith channel thread")
          log.trace("Starting setup thread for %s", connectionActor.uuid)
          t.start()
        } else {
          log.error("Failed to connect to %s, suiciding actor %s: %s", addressString, connectionActor.uuid, f.getCause)
          require(maybeChannel.isEmpty)

          // and we die
          connectionActor.stop()
        }
        log.trace("Leaving channel listener for %s", connectionActor.uuid)
      }
    })
  }

  override def preStart = {
    log.trace("preStart on %s", self.uuid)
    // this will suspend receiving messages until channel is open,
    // if the pool we're in uses a work-stealing dispatcher, then
    // other connections should get those messages instead.
    startOpeningChannel()
    log.trace("leaving preStart on %s", self.uuid)
  }

  override def postStop = {
    log.trace("postStop on %s", self.uuid)
    failAllPending(ConnectionFailure(new Exception("Connection to %s stopped".format(addressString))))
  }

  override def receive = {
    case incoming: Incoming =>
      log.trace("%s incoming message %s", self.uuid, incoming)
      incoming match {
        // message is from the app
        case clientWriteMessage: SendClientMessage => {
          sendMessageToMongo(self.channel, clientWriteMessage)
        }
      }
      log.trace("Post-send, senders waiting for reply: %s", senders)
    case netty: IncomingFromNetty =>
      log.trace("%s incoming netty %s", self.uuid, netty)
      netty match {
        case ServerMessageReceived(message) => {
          message match {
            case reply: ReplyMessage =>
              handleReplyMessage(reply)
          }
          log.trace("Post-handle-reply, waiting for reply: %s", senders)
        }
        case ChannelError(exception) => {
          log.trace("channel error on %s: %s %s", self.uuid, exception.getClass.getName, exception.getMessage)
          val failMessage = ConnectionFailure(exception)
          failAllPending(failMessage)
        }
        case ChannelClosed => {
          log.trace("channel close on %s", self.uuid)
          val failMessage = ConnectionFailure(new Exception("Channel %s is closed".format(addressString)))
          failAllPending(failMessage)
        }
      }
  }

  private def failAllPending(failMessage: ConnectionFailure) = {
    log.trace("Failing all pending senders: %s", senders)
    val oldSenders = senders
    senders = Map()
    oldSenders foreach { kv =>
      kv._2.channel ! failMessage
    }
  }

  private def handleReplyMessage(message: ReplyMessage) = {
    senders.get(message.header.responseTo) match {
      case Some(client) => {
        senders = senders - message.header.responseTo
        val actorReply = client.outgoingReplyBuilder(message)
        log.trace("matched response to %s and sending reply %s",
          message.header.responseTo, actorReply)
        client.channel ! actorReply
        log.trace("matched response to %s and removed from senders: %s",
          message.header.responseTo, senders)
      }
      case None => {
        log.trace("nobody was interested in response %s", message.header.responseTo)
        log.trace("interested senders are: %s", senders)
      }
    }
  }

  private def sendMessageToMongo(senderChannel: AkkaChannel[Any], clientRequest: SendClientMessage): Unit = {
    val doNothing = clientRequest match {
      case r: SendClientCheckMasterMessage =>
        !r.force
      case _ =>
        false
    }
    if (doNothing)
      return

    val channel = maybeChannel.get

    if (!channel.isConnected) {
      senderChannel ! ConnectionFailure(new Exception("Channel is closed."))
      return
    }
    require(channel.isConnected, "Channel is closed.")

    // if no reply builder, then it's fire-and-forget, no reply
    val maybeReplyBuilder = clientRequest match {
      case r: SendClientCheckMasterMessage => {
        require(r.force)
        Some(ConnectionActor.buildCheckMasterReply(_))
      }
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
        None // fire and forget,  no reply to this one
      case r: SendClientSingleWriteMessage =>
        Some(ConnectionActor.buildWriteReply(_))
      case r: SendClientBatchWriteMessage =>
        Some(ConnectionActor.buildBatchWriteReply(_))
    }

    val maybeWriteMessage = clientRequest match {
      case r: SendClientWriteMessage =>
        Some(r.message)
      case _ =>
        None
    }
    val concern = clientRequest match {
      case r: SendClientWriteMessage =>
        r.concern
      case _ =>
        WriteConcern.Normal
    }

    val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))
    log.trace("PreWrite with outStream '%s'", outStream)

    clientRequest.message.write(outStream)
    log.debug("Writing Message %s '%s' out to stream '%s' which we'll write to channel momentarily",
      clientRequest.message.requestID, clientRequest.message, outStream)

    if (maybeReplyBuilder.isDefined && maybeWriteMessage.isDefined) {
      if (concern.safe_?) {
        // we need to do a GetLastError for a safe write
        val wMsg = maybeWriteMessage.get
        val gle = createCommand(wMsg.namespace.split("\\.")(0), Document("getlasterror" -> 1))
        log.trace("Created a GetLastError Message: %s", gle)
        // we reply to the original SendClientWriteMessage when we get the GLE reply.
        // the write itself has no reply
        senders = senders + Pair(gle.requestID, ClientSender(senderChannel, maybeReplyBuilder.get))
        gle.write(outStream)
        log.debug("Wrote a getLastError with request ID %s", gle.requestID)
      } else {
        // if unsafe, we can just generate a reply here and now saying it "succeeded"
        // and go ahead and send the reply, no need to add to "senders"
        val writeReply = clientRequest match {
          case r: SendClientSingleWriteMessage =>
            WriteReply(r.message.ids.headOption, WriteResult(true))
          case r: SendClientBatchWriteMessage =>
            BatchWriteReply(Some(r.message.ids), WriteResult(true))
          case _ =>
            throw new Exception("this should not be possible, write message was not one")
        }
        log.trace("Sending immediate reply %s to unsafe write request ID %s",
          writeReply, clientRequest.message.requestID)
        senderChannel ! writeReply
      }
    } else {
      // for non-writes, if there's a reply builder we save it in "senders"
      maybeReplyBuilder match {
        case Some(builder) => {
          senders = senders + Pair(clientRequest.message.requestID, ClientSender(senderChannel, builder))
        }
        case None => {
          log.trace("No reply builder for request %s, not saving in senders",
            clientRequest.message.requestID)
        }
      }
    }

    channel.write(outStream.buffer())
    outStream.close()
  }
}

private[mongodb] object ConnectionChannelActor {

  // These are some extra messages specific to the netty channel,
  // that plain ConnectionActor doesn't support. We also get all
  // the ConnectionActor messages.
  sealed trait IncomingFromNetty
  // from netty thread
  case class ServerMessageReceived(message: MongoServerMessage) extends IncomingFromNetty
  // an error sent to us from netty thread
  case class ChannelError(t: Throwable) extends IncomingFromNetty
  // connection closed in netty thread
  case object ChannelClosed extends IncomingFromNetty

  /**
   * Factory for client socket channels, reused by all connections. Since it is shared,
   * releaseExternalResources() should never be called on this or on any bootstrap objects.
   */
  val channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(ThreadFactories("Hammersmith Netty Boss")),
    Executors.newCachedThreadPool(ThreadFactories("Hammersmith Netty Worker")))

  /* Pipeline factory generates a pipeline with our decoder and handler */
  class ConnectionActorPipelineFactory(val connectionActor: ActorRef,
    val addressString: String) extends ChannelPipelineFactory {

    private val actorHandler = new ConnectionActorHandler(connectionActor, addressString)
    private val setupHandler = new ConnectionSetupHandler(addressString)
    private val pipeline = Channels.pipeline(new ReplyMessageDecoder(), setupHandler)
    private var setup = false

    override def getPipeline = pipeline

    def awaitSetup() {
      require(!setup)
      setupHandler.await()
      // now swap in the real handler
      pipeline.replace(setupHandler, "actorHandler", actorHandler)
      setup = true
    }

    // can only call these after awaitSetup
    def setupFailed = setupHandler.failed
    def setupFailure = setupHandler.failure
    def isMaster = setupHandler.isMaster
    def maxBSONObjectSize = setupHandler.maxBSONObjectSize
  }

  /* Handler that we install first to set up (before the actor wants messages),
   * and then we replace it with the real handler.
   *
   * CAUTION: Do not give this thing a reference to the actor, because
   * while this is running, we rely on the waiting-for-setup thread
   * being the only thread touching the actor.
   */
  class ConnectionSetupHandler(val addressString: String)
    extends SimpleChannelHandler with Logging {

    private val readyLatch = new CountDownLatch(1)

    private var maybeFailure: Option[Throwable] = None

    private var maybeMaxBSONObjectSize: Option[Int] = None
    private var maybeIsMaster: Option[Boolean] = None
    private var connected = false

    def maxBSONObjectSize = maybeMaxBSONObjectSize.get
    def isMaster = maybeIsMaster.get
    def failed = maybeFailure.isDefined
    def failure = maybeFailure.get

    def await() = {
      readyLatch.await()
      log.trace("Setup handler await() complete")
    }

    override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) = {
      log.trace("setup handler incoming on channel: %s", e)
      super.handleUpstream(ctx, e)
    }

    override def handleDownstream(ctx: ChannelHandlerContext, e: ChannelEvent) = {
      log.trace("setup handler outgoing on channel: %s", e)
      super.handleDownstream(ctx, e)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val reply = e.getMessage.asInstanceOf[ReplyMessage]
      log.debug("Message received on setup handler (%s) assuming it's a reply to isMaster command", reply)

      val (m, b) = ConnectionActor.parseCheckMasterReply(reply)
      maybeIsMaster = Some(m)
      maybeMaxBSONObjectSize = Some(b)

      log.trace("isMaster reply was isMaster=%s bson object size %s", maybeIsMaster.get, maybeMaxBSONObjectSize.get)

      require(maybeMaxBSONObjectSize.isDefined &&
        maybeIsMaster.isDefined &&
        connected)

      log.trace("Setup handler is ready, counting down the latch")
      readyLatch.countDown()
    }

    private def fail(exception: Throwable) = {
      maybeFailure = Some(exception)
      log.trace("Setup handler logged failure, counting down the latch: %s", exception.getMessage)
      readyLatch.countDown()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error(e.getCause, "Uncaught exception in channel setup: %s", e.getCause)
      fail(e.getCause)
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.warn("Disconnected from '%s' in setup", addressString)
      fail(new Exception("Disconnected from mongod at " + addressString))
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("Channel Closed to '%s' in setup", addressString)
      fail(new Exception("Channel closed during setup " + addressString))
    }

    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("Connected to '%s' (Configging Channel to Little Endian)", addressString)
      e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))

      connected = true

      // Need to check master and bson size to proceed.
      // Send the query here.
      log.trace("Sending isMaster command on channel %s", ctx.getChannel)
      val qMsg = ConnectionActor.createCommand("admin", Document("isMaster" -> 1))
      val outStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))
      qMsg.write(outStream)(MongoMessage.DefaultMaxBSONObjectSize)
      ctx.getChannel.write(outStream.buffer())
      outStream.close()
    }
  }

  /* Connection handler forwards netty stuff to our actor.
   * Installed only after the setup handler is done handling initial
   * setup.
   */
  class ConnectionActorHandler(val connectionActor: ActorRef,
    val addressString: String)
    extends SimpleChannelHandler with Logging {

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val reply = e.getMessage.asInstanceOf[ReplyMessage]
      log.debug("Reply Message Received: %s", reply)
      connectionActor ! ConnectionChannelActor.ServerMessageReceived(reply)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error(e.getCause, "Uncaught exception Caught in ConnectionHandler: %s", e.getCause)
      connectionActor ! ConnectionChannelActor.ChannelError(e.getCause)
    }

    override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.warn("Disconnected from '%s'", addressString)
      connectionActor ! ConnectionChannelActor.ChannelError(new Exception("Disconnected from mongod at " + addressString))
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.info("Channel Closed to '%s'", addressString)
      connectionActor ! ConnectionChannelActor.ChannelClosed
    }
  }
}
