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
import org.bson._
import org.bson.collection._
import com.mongodb.async.wire._
import scala.collection.JavaConversions._
import com.mongodb.async.futures._
import org.bson.types.ObjectId
import org.bson.util.Logging
import com.mongodb.async.util._
import akka.actor.{ Channel => _, _ }
import akka.dispatch._

// this is needed because "implicit ActorRef" is too dangerous, need a dedicated type
// to be implicitly supplied
protected[mongodb] case class ConnectionActorHolder(actor: ActorRef)

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

  log.info("Initializing MongoConnection.")

  protected val connectionActor: ActorRef

  // lazy to avoid forcing connection actor to be created during construct
  implicit protected lazy val connectionActorHolder = ConnectionActorHolder(connectionActor)

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   */
  protected[mongodb] def runCommand[Cmd <% BSONDocument](ns: String, cmd: Cmd)(f: SingleDocQueryRequestFuture) {
    val qMsg = MongoConnection.createCommand(ns, cmd)
    log.trace("Created Query Message: %s, id: %d", qMsg, qMsg.requestID)
    send(qMsg, f)
  }

  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture)(implicit concern: WriteConcern = this.writeConcern) =
    MongoConnection.send(msg, f)

  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def apply(dbName: String): DB = database(dbName)

  /**
   * Remember, a DB is basically a future since it doesn't have to exist.
   */
  def database(dbName: String): DB = new DB(dbName)(this)

  def databaseNames(callback: Seq[String] => Unit) {
    runCommand("admin", Document("listDatabases" -> 1))(SimpleRequestFutures.command((doc: Document) => {
      log.trace("Got a result from 'listDatabases' command: %s", doc)
      if (!doc.isEmpty) {
        val dbs = {
          val lst = doc.as[BSONList]("databases").asList
          lst.map(_.asInstanceOf[Document].as[String]("name"))
        }
        callback(dbs)
      } else {
        log.warning("Command 'listDatabases' failed. Doc: %s", doc)
        callback(List.empty[String])
      }
    }))
  }

  /**
   * Asks the server if we are still the master connection,
   * blocking for a reply. After this returns, the isMaster
   * field could have a new value.
   */
  def checkMaster() = {
    log.debug("Checking Master Status...")
    val futureReply: Future[Any] = connectionActor !!! ConnectionActor.SendClientCheckMasterMessage
    futureReply.get match {
      case ConnectionActor.CheckMasterReply(isMaster, maxBSONObjectSize) =>
      // the actor will have already updated isMaster, though it would be
      // a race to assert that here.
    }
  }

  /**
   * Throws an exception if we aren't the master.
   */
  def throwIfNotMaster() = {
    if (!isMaster)
      throw new Exception("Connection is required to be master and is not")
  }

  /**
   * Checks if the connection is connected. Almost any conceivable use
   * of this method will create a race, because the connection can disconnect
   * between checking this and any operation you perform. So, the main use
   * of this is probably debugging.
   *
   * On direct connections this has much more meaning than on connection pools.
   * On anything other than a single-socket direct connection, the results
   * of this method are not well-defined.
   */
  def connected_? : Boolean

  /**
   * Checks if the connection is a master. Can change at any time, so
   * many if not most uses of this are probably races.
   *
   * Also, in theory if the connection is a pool, the sockets in the
   * pool may not agree or may not all agree simulataneously on this
   * flag, and isMaster for the pool may not have well-defined behavior.
   */
  def isMaster: Boolean

  val addr: InetSocketAddress

  protected[mongodb] var _writeConcern: WriteConcern = WriteConcern.Normal

  def close() {
    log.info("Closing down connection.")
    connectionActor.stop()
  }

  /**
   *
   * Set the write concern for this database.
   * Will be used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * @param concern (WriteConcern) The write concern to use
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/mongodb-single-server-data-durability-guide/
   */
  def writeConcern_=(concern: WriteConcern) = _writeConcern = concern

  /**
   *
   * get the write concern for this database,
   * which is used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/mongodb-single-server-data-durability-guide/
   */
  def writeConcern = _writeConcern

  /**
   * Returns a connection that wraps a single socket to a single MongoDB instance.
   * This is needed if you are going to do a series of commands that need to
   * happen in serial, i.e. one command needs to see the results from the previous,
   * such as an insert then a count, or a write then a getLastError.
   * In that case you'd want to use connection.direct in place of plain connection.
   */
  def direct: DirectConnection
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

  def apply(hostname: String = "localhost", port: Int = 27017): MongoConnection = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    new PoolConnection(new InetSocketAddress(hostname, port))
  }

  /**
   * Connect to MongoDB using a URI format.
   *
   * Because we can't tell if you are giving us just a host, a DB or a collection
   * The return type is a Triple of Connection, Option[DB], Option[Collection].
   *
   * You'll be given each of thes ethat could be validly built.
   *
   * @see http://www.mongodb.org/display/DOCS/Connections
   */
  def fromURI(uri: String): (MongoConnection, Option[DB], Option[Collection]) = uri match {
    case MongoURI(hosts, db, collection, username, password, options) => {
      require(hosts.size > 0, "No valid hosts found in parsed host list")
      if (hosts.size == 1) {
        val _conn = MongoConnection(hosts.head._1, hosts.head._2)
        // TODO - Authentication!
        val _db = db match {
          case Some(dbName) => Some(_conn(dbName))
          case None => None
        }

        val _coll = collection match {
          case Some(collName) => {
            assume(_db.isDefined, "Cannot specify a collection name with no DB Name")
            Some(_db.get.apply(collName))
          }
          case None => None
        }
        (_conn, _db, _coll)
      } else throw new UnsupportedOperationException("No current support for multiple hosts in this driver")
    }
  }

  private def completeRequestFuture(f: RequestFuture, reply: ConnectionActor.Outgoing) = {
    (reply, f) match {
      case (ConnectionActor.CursorReply(cursorActor), rf: CursorQueryRequestFuture) =>
        rf(Cursor[rf.DocType](cursorActor)(rf.decoder).asInstanceOf[rf.T])
      case (ConnectionActor.GetMoreReply(cursorId, docs), rf: GetMoreRequestFuture) =>
        rf(cursorId, docs map { doc => rf.decoder.decode(doc) })
      case (ConnectionActor.SingleDocumentReply(doc), rf: SingleDocQueryRequestFuture) =>
        rf(rf.decoder.decode(doc))
      case (ConnectionActor.OptionalSingleDocumentReply(maybeDoc), rf: FindAndModifyRequestFuture) =>
        rf(rf.decoder.decode(maybeDoc.get)) // FIXME the FindAndModifyRequestFuture should take an Option?
      case (ConnectionActor.WriteReply(maybeId, result), rf: WriteRequestFuture) =>
        rf((maybeId, result).asInstanceOf[rf.T]) // something is busted that we need this cast
      case (ConnectionActor.BatchWriteReply(maybeIds, result), rf: BatchWriteRequestFuture) =>
        rf((maybeIds, result).asInstanceOf[rf.T]) // something is busted that we need this cast
      case (_, NoOpRequestFuture) => // silence compiler, can't happen
    }
  }

  /*
   * This whole mess could be deleted in an API-breaking phase 2, in which
   * we'd use Akka futures and eliminate the RequestFuture.
   */
  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture)(implicit connectionActorHolder: ConnectionActorHolder, concern: WriteConcern): Unit = {
    val connectionActor = connectionActorHolder.actor
    val actorMessage: ConnectionActor.Incoming =
      (msg, f) match {
        case (m: QueryMessage, rf: CursorQueryRequestFuture) =>
          ConnectionActor.SendClientCursorMessage(m)
        case (m: GetMoreMessage, rf: GetMoreRequestFuture) =>
          ConnectionActor.SendClientGetMoreMessage(m)
        case (m: QueryMessage, rf: SingleDocQueryRequestFuture) =>
          ConnectionActor.SendClientSingleDocumentMessage(m)
        case (m: QueryMessage, rf: FindAndModifyRequestFuture) =>
          ConnectionActor.SendClientOptionalSingleDocumentMessage(m)
        case (m: MongoClientWriteMessage, rf: WriteRequestFuture) =>
          ConnectionActor.SendClientSingleWriteMessage(m, concern)
        case (m: MongoClientWriteMessage, rf: BatchWriteRequestFuture) =>
          ConnectionActor.SendClientBatchWriteMessage(m, concern)
        case (m: KillCursorsMessage, NoOpRequestFuture) =>
          ConnectionActor.SendClientKillCursorsMessage(m)
      }
    if (f == NoOpRequestFuture) {
      connectionActor ! actorMessage
    } else {
      val replyFuture: Future[Any] = connectionActor !!! actorMessage
      replyFuture.onComplete({ replyFuture =>
        try {
          replyFuture.get match {
            case o: ConnectionActor.Outgoing =>
              completeRequestFuture(f, o)
          }
        } catch {
          case e: Throwable =>
            f(e)
        }
      })
    }
  }

  protected[mongodb] def createCommand[Cmd <% BSONDocument](ns: String, cmd: Cmd) = {
    log.trace("Attempting to create command '%s' on DB '%s.$cmd'", cmd, ns)
    QueryMessage(ns + ".$cmd", 0, -1, cmd)
  }
}

