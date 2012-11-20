package com.mongodb.async
package netty

/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import com.mongodb.async.MongoConnection

import com.mongodb.async.util._
import com.mongodb.async.wire._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{Channel, AdaptiveReceiveBufferSizePredictor, Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.execution.ExecutionHandler
import java.util.concurrent.atomic.AtomicBoolean
import java.net.InetSocketAddress
import java.io.OutputStream
import java.nio.ByteOrder
import org.jboss.netty.buffer.{ChannelBufferOutputStream, ChannelBuffers}
import org.bson.util.Logging
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.buffer._


class NettyConnection(val addr: InetSocketAddress) extends MongoConnection {

  type E = Executor

  log.info("Initializing Netty-based MongoDB connection on address '%s'", addr)

  lazy val initialize: ConnectionContext = {

    /*
     * Factory for client socket channels,
     *
     * TODO - reuse with all connectors where possible.
     *
     */
    val channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(ThreadFactories("Hammersmith Netty Boss")),
      Executors.newCachedThreadPool(ThreadFactories("Hammersmith Netty Worker")))

    val bootstrap = new ClientBootstrap(channelFactory)

    val handler = new NettyConnectionHandler(bootstrap)

    log.trace("Event Loop: " + eventLoop + " empty? " + eventLoop.isEmpty + " getOrElse? " + eventLoop.getOrElse(defaultEventLoop))
    
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      private val appCallbackExecutionHandler =
        new ExecutionHandler(eventLoop.getOrElse(defaultEventLoop))

      def getPipeline = {
        val p = Channels.pipeline(new ReplyMessageDecoder(),
          appCallbackExecutionHandler,
          handler)
        p
      }
    })

    
    bootstrap.setOption("remoteAddress", addr)
    /* AdaptiveReceiveBufferSizePredictor gradually scales the buffer up and down
     * depending on how many bytes arrive in each read()
     */
    bootstrap.setOption("child.receiveBufferSizePredictor",
      new AdaptiveReceiveBufferSizePredictor(128, /* minimum */
        256, /* initial */
        1024 * 1024 * 4 /* max */ ))

    val _f = bootstrap.connect()
    // TODO - Switch to listener based establishment
    val channel = _f.awaitUninterruptibly.getChannel

    val ctx = new NettyConnectionContext(channel)

    MongoConnection.connectionState.put(ctx, new AtomicBoolean(false))
    


    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)
    if (!_f.isSuccess) {
      log.error("Failed to connect.", _f.getCause)
      bootstrap.releaseExternalResources()
      throw new Exception("Unable to connect to MongoDB.", _f.getCause)
      // TODO: Retry system
    }
    log.info("Connected and retrieved a write channel (%s)", channel)
    handler.ctx = ctx
    ctx
  }


  /* The executor ensures that we use more than one thread, so apps
  * that call back into hammersmith from a callback don't deadlock,
  * and so apps can use CPU (e.g. decoding) without slowing down IO.
  *
  * This executor does nothing to preserve order of message
  * processing.
  * By going unordered, we can decode replies and
  * run app callbacks in parallel. That seems like a pretty
  * big win; otherwise, we can only use one CPU to decode and
  * process replies.
  * (Replies from a connection pool would be in
  * undefined order anyhow from the app's perspective,
  * since the app doesn't know which socket the request
  * went to.)
  *
  * Netty comes with a MemoryAwareThreadPoolExecutor and
  * OrderedMemoryAwareThreadPoolExecutor. These have
  * two problems. First, they use a queue,
  * which means they never go above CorePoolSize
  * (the queue has a fixed upper limit). This would create
  * a deadlock if CorePoolSize threads are busy and the app
  * calls back to Hammersmith. Moreover, the memory limit
  * can create a deadlock if it stops accepting more messages
  * and the app calls back to Hammersmith. Basically we can never
  * stop processing messages or there's a deadlock.
  * So we don't use the executors from Netty, instead using a plain
  * ThreadPoolExecutor.
  *
  * Actors could be better than threads, in the future. Unlike
  * invoking a callback, sending a message to an actor should not
  * tie up the pipeline and risk deadlock.
  */
  lazy val defaultEventLoop =
    new ThreadPoolExecutor(Runtime.getRuntime.availableProcessors * 2, /* core pool size */
      Int.MaxValue, /* max pool size (must be infinite to avoid deadlocks) */
      20, TimeUnit.SECONDS, /* time to keep idle threads alive */
      new SynchronousQueue[Runnable], /* queue that doesn't queue; we must make a thread, or we could deadlock */
      ThreadFactories("Hammersmith Reply Handler"))

}


class NettyConnectionContext protected[mongodb](private val channel: Channel) extends ConnectionContext with Logging {
  
  
  def close() {
    channel.close()
  }
  
  def newOutputStream: OutputStream = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer(ByteOrder.LITTLE_ENDIAN, 256))

  def connected_?(): Boolean = channel.isConnected

  def write(msg: AnyRef) = msg match {
    case os: ChannelBufferOutputStream =>
      channel.write(os.buffer())
    case default =>
      // attempt to write it anyway
      channel.write(default)
  }
}

protected[mongodb] class NettyConnectionHandler(val bootstrap: ClientBootstrap) extends SimpleChannelHandler with MongoConnectionHandler {

  override def messageReceived(nettyCtx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage.asInstanceOf[MongoMessage]
    log.debug("Incoming Message received type %s", message.getClass.getName)
    receiveMessage(message)
  }
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    // TODO - pass this up to the user layer?
    log.error(e.getCause, "Uncaught exception Caught in ConnectionHandler: %s", e.getCause)
    // TODO - Close connection?
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.warn("Disconnected from '%s'", remoteAddress)
    //    shutdown()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Channel Closed to '%s'", remoteAddress)
    //    shutdown()
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Connected to '%s' (Configging Channel to Little Endian)", remoteAddress)
    e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))
  }

  def remoteAddress = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]
}

