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

import org.bson.util.Logging
import org.bson.collection._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import com.mongodb.async.wire._
import com.mongodb.async.futures._
import org.jboss.netty.buffer._
import java.net.InetSocketAddress
import java.nio.ByteOrder
import akka.actor.ActorRef
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
abstract class MongoConnectionHandler extends SimpleChannelHandler with Logging {
  protected val addressString: String
  protected val connectionActor: ActorRef

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage.asInstanceOf[MongoMessage]
    log.debug("Incoming Message received type %s", message.getClass.getName)
    message match {
      case reply: ReplyMessage => {
        log.debug("Reply Message Received: %s", reply)
        connectionActor ! ConnectionChannelActor.ServerMessageReceived(reply)
      }
      case default => {
        log.warn("Unknown message type '%s'; ignoring.", default)
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    // TODO - pass this up to the user layer?
    log.error(e.getCause, "Uncaught exception Caught in ConnectionHandler: %s", e.getCause)
    connectionActor ! ConnectionChannelActor.ChannelError(e.getCause)
    // TODO - Close connection?
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.warn("Disconnected from '%s'", addressString)
    connectionActor ! ConnectionChannelActor.ChannelError(new Exception("Disconnected from mongod at " + addressString))
    //    shutdown()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Channel Closed to '%s'", addressString)
    connectionActor ! ConnectionChannelActor.ChannelClosed
    //    shutdown()
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Connected to '%s' (Configging Channel to Little Endian)", addressString)
    e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))
  }
}

