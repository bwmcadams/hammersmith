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

  protected def queryFail(reply: ReplyMessage, result: RequestFuture) = {
    log.trace("Query Failure")
    // Attempt to grab the $err document
    val err = reply.documents.headOption match {
      case Some(b) ⇒ {
        val errDoc = SerializableDocument.decode(b) // TODO - Extractors!
        log.trace("Error Document found: %s", errDoc)
        result(new Exception(errDoc.getAsOrElse[String]("$err", "Unknown Error.")))
      }
      case None ⇒ {
        log.warn("No Error Document Found.")
        "Unknown Error."
        result(new Exception("Unknown error."))
      }
    }
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage.asInstanceOf[MongoMessage]
    log.debug("Incoming Message received type %s", message.getClass.getName)
    message match {
      case reply: ReplyMessage ⇒ {
        log.debug("Reply Message Received: %s", reply)
        // Dispatch the reply, separate into requisite parts, etc
        /**
         * Note - it is entirely OK to stuff a single result into
         * a Cursor, but not multiple results into a single.  Should be obvious.
         */
        val req = MongoConnection.dispatcher.get(reply.header.responseTo)
        log.trace("[%s] Req Obj: %s", reply.header.responseTo, req)
        /**
         * Even when no response is wanted a 'Default' callback should be regged so
         * this is definitely warnable, for now.
         */
        if (req.isEmpty) log.warn("No registered callback for request ID '%d'.  This may or may not be a bug.", reply.header.responseTo)
        req.foreach(_ match {
          case _r: CompletableReadRequest ⇒ _r match {
            case CompletableSingleDocRequest(msg: QueryMessage, singleResult: SingleDocQueryRequestFuture) ⇒ {
              log.debug("Single Document Request Future. Decoder: %s.", _r.decoder)
              // This may actually be better as a disableable assert but for now i want it hard.
              require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a SingleDocQueryRequestFuture.")
              // Check error state
              if (reply.cursorNotFound) {
                log.trace("Cursor Not Found.")
                singleResult(new Exception("Cursor Not Found."))
              } else if (reply.queryFailure) {
                queryFail(reply, singleResult)
              } else {
                val doc = reply.documents.head
                import org.bson.io.Bits._
                singleResult(_r.decoder.decode(reply.documents.head).asInstanceOf[singleResult.T]) // TODO - Fix me!
              }
            }
            case CompletableCursorRequest(msg: QueryMessage, cursorResult: CursorQueryRequestFuture) ⇒ {
              log.trace("Cursor Request Future.")
              if (reply.cursorNotFound) {
                log.trace("Cursor Not Found.")
                cursorResult(new Exception("Cursor Not Found."))
              } else if (reply.queryFailure) {
                queryFail(reply, cursorResult)
              } else {
                cursorResult(Cursor(msg.namespace, reply)(ctx, _r.decoder).asInstanceOf[cursorResult.T]) // TODO - Fix Me!
              }
            }
            case CompletableGetMoreRequest(msg: GetMoreMessage, getMoreResult: GetMoreRequestFuture) ⇒ {
              log.trace("Get More Request Future.")
              if (reply.cursorNotFound) {
                log.warn("Cursor Not Found.")
                getMoreResult(new Exception("Cursor Not Found"))
              } else if (reply.queryFailure) {
                queryFail(reply, getMoreResult)
              } else {
                getMoreResult((reply.cursorID, reply.documents.map(_r.decoder.decode)).asInstanceOf[getMoreResult.T]) // TODO - Fix Me!
              }
            }
          }
          // TODO - Handle any errors in a "non completable" // TODO - Capture generated ID? the _ids thing on insert is not quite ... defined.
          case CompletableWriteRequest(msg, writeResult: WriteRequestFuture) ⇒ {
            log.trace("Write Request Future.")
            require(reply.numReturned <= 1, "Found more than 1 returned document; cannot complete a WriteRequestFuture.")
            // Check error state
            // Attempt to grab the document
            reply.documents.headOption match {
              case Some(b) ⇒ {
                val doc = SerializableDocument.decode(b) // TODO - Extractors!
                log.debug("Document found: %s", doc)
                val ok = boolCmdResult(doc, false)
                // this is how the Java driver decides to throwOnError, !ok || "err"
                val failed = !ok || doc.get("err").isDefined
                if (failed) {
                  // when does mongodb use errmsg vs. err ?
                  val errmsg = doc.getAsOrElse[String]("errmsg", doc.getAs[String]("err").get)
                  // FIXME should be a custom exception type probably
                  writeResult(new Exception(errmsg))
                } else {
                  val w = WriteResult(ok = true,
                    error = None,
                    // FIXME is it possible to have a code but no error?
                    code = doc.getAs[Int]("code"),
                    n = doc.getAsOrElse[Int]("n", 0),
                    upsertID = doc.getAs[AnyRef]("upserted"),
                    updatedExisting = doc.getAs[Boolean]("updatedExisting"))
                  log.debug("W: %s", w)
                  writeResult((None, w).asInstanceOf[writeResult.T])
                }
              }
              case None ⇒ {
                log.warn("No Document Found.")
                "Unknown Error."
                writeResult(new Exception("Unknown error; no document returned.."))
              }
            }
          }

          case unknown ⇒ log.error("Unknown or unexpected value in dispatcher map: %s", unknown)
        })
      }
      case default ⇒ {
        log.warn("Unknown message type '%s'; ignoring.", default)
      }
    }
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

