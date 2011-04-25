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
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import com.mongodb.async.wire._
import com.mongodb.async.futures._
import org.jboss.netty.buffer._
import scala.collection.mutable.{Set => MutableSet}
import java.net.InetSocketAddress
import java.nio.ByteOrder

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
  protected val bootstrap: ClientBootstrap
  protected[mongodb] var maxBSONObjectSize = 1024 * 4 * 4 // default

  log.info("Initializing MongoConnectionHandler.")
  MongoConnection.cleaningTimer.acquire(this)


  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val buf = e.getMessage.asInstanceOf[ChannelBuffer]
    log.debug("Incoming Message received on (%s) Length: %s", buf, buf.readableBytes())
    MongoMessage.unapply(new ChannelBufferInputStream(buf)) match {
      case reply: ReplyMessage => {
        log.trace("Reply Message Received: %s", reply)
        // Dispatch the reply, separate into requisite parts, etc
        /**
         * Note - it is entirely OK to stuff a single result into
         * a Cursor, but not multiple results into a single.  Should be obvious.
         */
        val req = MongoConnection.dispatcher.get(reply.header.responseTo)
        /**
         * Even when no response is wanted a 'Default' callback should be regged so
         * this is definitely warnable, for now.
         */
        if (req.isEmpty) log.warn("No registered callback for request ID '%d'.  This may or may not be a bug.", reply.header.responseTo)
        req.foreach(_ match {
          case CompletableSingleDocRequest(msg: QueryMessage, singleResult: SingleDocQueryRequestFuture) => {
            log.trace("Single Document Request Future.")
            // This may actually be better as a disableable assert but for now i want it hard.
            require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a SingleDocQueryRequestFuture.")
            // Check error state    k
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
          case CompletableCursorRequest(msg: QueryMessage, cursorResult: CursorQueryRequestFuture) => {
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
          case CompletableGetMoreRequest(msg: GetMoreMessage, getMoreResult: GetMoreRequestFuture) => {
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
          case unknown => log.error("Unknown or unexpected value in dispatcher map: %s", unknown)
        })
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
    shutdown()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Channel Closed to '%s'", remoteAddress)
    shutdown()
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    log.info("Connected to '%s' (Configging Channel to Little Endian)", remoteAddress)
    e.getChannel.getConfig.setOption("bufferFactory", new HeapChannelBufferFactory(ByteOrder.LITTLE_ENDIAN))
  }

  def remoteAddress = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]

  /**
   * Cursors that need to be cleaned up
   */
  protected val deadCursors = MutableSet.empty[Long]

  /**
   * Deferred - doesn't actually happen immediately
   */
  protected[mongodb] def killCursors(ids: Long*) {
    log.debug("Adding Dead Cursors to cleanup list: %s", ids)
    deadCursors ++= ids
  }

  /**
   *  Clean up any resources (Typically cursors)
   *  Called regularly by a managed CursorCleaner thread.
   */
  protected[mongodb] def cleanup() {
    log.debug("Cursor Cleanup running.")
  }


  protected[mongodb] def shutdown() {
    log.debug("Shutting Down & Cleaning up connection handler.")
    MongoConnection.cleaningTimer.stop(this)
  }
}




