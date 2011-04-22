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

import java.net.InetSocketAddress

import org.bson.util.Logging
import org.bson.collection._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.nio.ByteOrder
import com.mongodb.async.wire._
import com.mongodb.async.futures._
import org.jboss.netty.buffer._
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
trait MongoConnectionHandler extends SimpleChannelHandler with Logging {
  val bootstrap: ClientBootstrap

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val buf = e.getMessage.asInstanceOf[ChannelBuffer]
    log.debug("Incoming Message received on (%s) Length: %s", buf, buf.readableBytes())
    val _msg = MongoMessage.unapply(new ChannelBufferInputStream(buf));
    _msg match {
      case reply: ReplyMessage => {
        log.trace("Reply Message Received: %s", reply)
        // Dispatch the reply, separate into requisite parts, etc
        /**
         * Note - it is entirely OK to stuff a single result into
         * a Cursor, but not multiple results into a single.  Should be obvious.
         */
        MongoConnection.dispatcher.get(reply.header.responseTo) match {
          case Some(CompletableRequest(msg: QueryMessage, singleResult: SingleDocQueryRequestFuture)) => {
            log.trace("Single Document Request Future.")
            // This may actually be better as a disableable assert but for now i want it hard.
            require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a SingleDocQueryRequestFuture.")
            // Check error state
            // TODO - Different handling depending on type of op, GetLastError etc
            // Though - GetLastError could dispatch back out again here and not invoke the callback!
            if (reply.cursorNotFound) {
              log.trace("Cursor Not Found.")
              singleResult(new Exception("Cursor Not Found."))
            } else if (reply.queryFailure) {
              log.trace("Query Failure")
              // Attempt to grab the $err document
              val err = reply.documents.headOption match {
                case Some(errDoc) => {
                  log.trace("Error Document found: %s", errDoc)
                  singleResult(new Exception(errDoc.getAsOrElse[String]("$err", "Unknown Error.")))
                }
                case None => {
                  log.warn("No Error Document Found.")
                  "Unknown Error."
                  singleResult(new Exception("Unknown error."))
                }
              }
            } else {
              singleResult(reply.documents.head.asInstanceOf[singleResult.T])
            }
          }
          case Some(CompletableRequest(msg: QueryMessage, cursorResult: CursorQueryRequestFuture)) => {
            log.trace("Cursor Request Future.")
            if (reply.cursorNotFound) {
              log.trace("Cursor Not Found.")
              cursorResult(new Exception("Cursor Not Found."))
            } else if (reply.queryFailure) {
              log.trace("Query Failure")
              // Attempt to grab the $err document
              val err = reply.documents.headOption match {
                case Some(errDoc) => {
                  log.trace("Error Document found: %s", errDoc)
                  cursorResult(new Exception(errDoc.getAsOrElse[String]("$err", "Unknown Error.")))
                }
                case None => {
                  log.warn("No Error Document Found.")
                  "Unknown Error."
                  cursorResult(new Exception("Unknown error."))
                }
              }
            } else {
              cursorResult(new Cursor(msg.namespace, reply)(ctx).asInstanceOf[cursorResult.T])
            }
          }
          case Some(CompletableRequest(msg: GetMoreMessage, getMoreResult: GetMoreRequestFuture)) => {
            log.trace("Get More Request Future.")
            if (reply.cursorNotFound) {
              log.warn("Cursor Not Found.")
              getMoreResult(new Exception("Cursor Not Found"))
            } else if (reply.queryFailure) {
              log.warn("Query Failure")
              // Attempt to grab the $err document
              val err = reply.documents.headOption match {
                case Some(errDoc) => {
                  log.debug("Error Document found: %s", errDoc)
                  getMoreResult(new Exception(errDoc.getAsOrElse[String]("$err", "Unknown Error.")))
                }
                case None => {
                  log.warn("No Error Document Found.")
                  getMoreResult(new Exception("Unknown Error."))
                }
              }
            } else {
              getMoreResult((reply.cursorID, reply.documents).asInstanceOf[getMoreResult.T])
            }
          }
          case Some(unknown) => log.error("Unknown or unexpected value in dispatcher map: %s", unknown)
          case None => {
            /**
             * Even when no response is wanted a 'Default' callback should be regged so
             * this is definitely warnable, for now.
             */
            log.warn("No registered callback for request ID '%d'.  This may or may not be a bug.",
              reply.header.responseTo)
          }
        }
      }
      case default => {
        log.warn("Unknown message type '%s'; ignoring.", default)
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    // TODO - pass this up to the user layer?
    log.error(e.getCause, "Uncaught exception Caught in ConnectionHandler")
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
    log.info("Connected to '%s' (Configging Channel to Little Endian)", remoteAddress)
    e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))
  }

  def remoteAddress = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]

  var maxBSONObjectSize = 1024 * 4 * 4 // default
}




