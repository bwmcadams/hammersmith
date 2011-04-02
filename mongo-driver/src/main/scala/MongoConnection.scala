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

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.util.concurrent.Executors
import java.net.InetSocketAddress

import com.mongodb.util.Logging
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel._
import com.mongodb.wire.BSONFrameDecoder

/**
 * Base trait for all connections, be it direct, replica set, etc
 *
 * This contains common code for any type of connection.
 *
 * NOTE: Connection instances are instances of a *POOL*, always.
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @since 0.1
 */
abstract class MongoConnection extends Logging {

  bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
    def getPipeline() = Channels.pipeline(new BSONFrameDecoder(), newHandler)
  })

  bootstrap.setOption("remoteAddress", addr)

  private val future = bootstrap.connect()
  protected val channel = future.awaitUninterruptibly.getChannel

  if (!future.isSuccess) {
    log.error("Failed to connect.", future.getCause)
    bootstrap.releaseExternalResources()
  } else log.info("Connected and retrieved a write channel (%s)", channel)

  /**
   * Utility method to pull back a number of pieces of information
   * including maxBSONObjectSize, and sort of serves as a verification
   * of a live connection.
   *
   * @param force Forces the isMaster call to run regardless of cached status
   * @param requireMaster Requires a master to be found or throws an Exception
   * @throws MongoException
   */
  def checkMaster(force: Boolean = false, requireMaster: Boolean = true) = {
    if (!haveMasterStatus || force) {
      log.debug("Checking Master Status... (Already Have? %s Force? %s)", haveMasterStatus, force)
      maxBSONObjectSize = readMaxBSONObjectSize()
    }
    log.debug("Already have cached master status. Skipping.")
  }

  def newHandler: DirectConnectionHandler

  val addr: InetSocketAddress

  def readMaxBSONObjectSize() = {
    0
  }

  // TODO - MAKE THESE IMMUTABLE
  /** Do we already have a cached master status? */
  var haveMasterStatus = false
  /** Maximum size of BSON this serve allows. */
  var maxBSONObjectSize = 0

  /* TODO - Can we reuse these factories across multiple connections??? */
  /**
   * Factory for client socket channels, reused by all connectors where possible.
   */
  val channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool,
    Executors.newCachedThreadPool)

  implicit val bootstrap = new ClientBootstrap(channelFactory)

  bootstrap.setOption("tcpNoDelay", true)
  bootstrap.setOption("keepAlive", true)

}

trait MongoConnectionHandler extends SimpleChannelHandler with Logging {
  val bootstrap: ClientBootstrap

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val buf = e.getMessage.asInstanceOf[ChannelBuffer]
    log.trace("Message Received: %s", buf)

    // TODO - decode me!
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    // TODO - pass this up to the user layer?
    log.error("Exception Caught in ConnectionHandler.", e)
    // TODO - Close connection?
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.warn("Disconnected from '%s'", remoteAddress)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Channel Closed to '%s'", remoteAddress)
    // TODO - Reconnect?
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Connected to '%s'", remoteAddress)
  }

  def remoteAddress = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]

}

/**
 * Factory object for creating connections
 * based on things like URI Spec
 *
 * NOTE: Connection instances are instances of a *POOL*, always.
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @since 0.1
 */
object MongoConnection extends Logging {

  def apply(hostname: String, port: Int = 27017) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    new DirectConnection(new InetSocketAddress(hostname, port))
  }

}

