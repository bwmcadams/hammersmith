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

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import java.net.InetSocketAddress
import org.bson._
import org.bson.collection._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import java.nio.ByteOrder
import com.mongodb.async.wire._
import scala.collection.JavaConversions._
import com.mongodb.async.futures._
import org.jboss.netty.buffer._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import org.bson.types.ObjectId
import org.bson.util.Logging
import com.mongodb.async.util._
import scala.collection.mutable.WeakHashMap
import com.mongodb.async.util.ConcurrentQueue
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

  private case class CheckMasterState(isMaster: Boolean, maxBSONObjectSize: Int)
  private var checkMasterState: Option[CheckMasterState] = None

  /**
   * Utility method to pull back a number of pieces of information
   * including maxBSONObjectSize, and sort of serves as a verification
   * of a live connection.
   *
   * FIXME what does this even mean when we have a pool of channels?
   * FIXME this blocks, but if it's async the API is messed up,
   *       since requireMaster can't mean anything.
   *
   * @param force Forces the isMaster call to run regardless of cached status
   * @param requireMaster Requires a master to be found or throws an Exception
   * @throws MongoException
   */
  def checkMaster(force: Boolean = false, requireMaster: Boolean = true) {
    if (!checkMasterState.isDefined || force) {
      log.info("Checking Master Status... (Force? %s)", force)
      // FIXME note this is sending CheckMaster to a random actor in the pool...
      val futureReply: Future[Any] = connectionActor !!! ConnectionActor.SendClientCheckMasterMessage(force)
      // we block here... maybe not great?
      futureReply.get match {
        case ConnectionActor.CheckMasterReply(isMaster, maxBSONObjectSize) =>
          checkMasterState = Some(CheckMasterState(isMaster, maxBSONObjectSize))
      }
    }
    if (requireMaster) {
      if (!(checkMasterState.isDefined && checkMasterState.get.isMaster)) {
        throw new Exception("Connection is required to be master and is not")
      }
    }
  }

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

  def find[Qry <: BSONDocument, Flds <: BSONDocument](db: String)(collection: String)(query: Qry = Document.empty, fields: Flds = Document.empty, numToSkip: Int = 0, batchSize: Int = 0)(callback: CursorQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    val qMsg = QueryMessage(db + "." + collection, numToSkip, batchSize, query, fieldSpec(fields))
    send(qMsg, callback)
  }

  def findOne[Qry <: BSONDocument, Flds <: BSONDocument](db: String)(collection: String)(query: Qry = Document.empty, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    val qMsg = QueryMessage(db + "." + collection, 0, -1, query, fieldSpec(fields))
    send(qMsg, callback)
  }

  // TODO - should we allow any and do boxing elsewhere?
  // TODO - FindOne is Option[] returning, ensure!
  def findOneByID[A <: AnyRef, Flds <: BSONDocument](db: String)(collection: String)(id: A, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture) =
    findOne(db)(collection)(Document("_id" -> id), fields)(callback)

  // TODO - Immutable mode / support immutable objects
  def insert[T](db: String)(collection: String)(doc: T, validate: Boolean = true)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    log.trace("Inserting: %s to %s.%s with WriteConcern: %s", doc, db, collection, concern)
    val checked = if (validate) {
      m.checkObject(doc)
      m.checkID(doc)
    } else {
      log.debug("Validation of objects disabled; no ID Gen.")
      doc
    }
    send(InsertMessage(db + "." + collection, checked), callback)
  }

  /**
   * Insert multiple documents at once.
   * Keep in mind, that WriteConcern behavior may be wonky if you do a batchInsert
   * I believe the behavior of MongoDB will cause getLastError to indicate the LAST error
   * on your batch ---- not the first, or all of them.
   *
   * The WriteRequest used here returns a Seq[] of every generated ID, not a single ID
   * TODO - Support turning off ID Validation
   */
  def batchInsert[T](db: String)(collection: String)(docs: T*)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    log.trace("Batch Inserting: %s to %s.%s with WriteConcern: %s", docs, db, collection, concern)
    val checked = docs.map(x => {
      m.checkObject(x)
      m.checkID(x)
    })
    send(InsertMessage(db + "." + collection, checked: _*), callback)
  }

  /**
   * Counts the number of documents in a given namespace
   * -1 indicates an error, for now
   */
  def count[Qry: SerializableBSONObject, Flds: SerializableBSONObject](db: String)(collection: String)(query: Qry = Document.empty,
    fields: Flds = Document.empty,
    limit: Long = 0,
    skip: Long = 0)(callback: Int => Unit) = {
    val builder = OrderedDocument.newBuilder
    builder += ("count" -> collection)
    builder += ("query" -> query)
    builder += ("fields" -> fields)
    if (limit > 0)
      builder += ("limit" -> limit)
    if (skip > 0)
      builder += ("skip" -> skip)
    runCommand(db, builder.result)(SimpleRequestFutures.command((doc: Document) => {
      log.trace("Got a result from 'count' command: %s", doc)
      callback(doc.getAsOrElse[Double]("n", -1.0).toInt)
    }))
  }

  /**
   * Calls findAndModify in remove only mode with
   * fields={}, sort={}, remove=true, getNew=false, upsert=false
   * @param query
   * @return the removed document
   */
  def findAndRemove[Qry: SerializableBSONObject](db: String)(collection: String)(query: Qry = Document.empty)(callback: FindAndModifyRequestFuture) = findAndModify(db)(collection)(query = query, remove = true, update = Option[Document](null))(callback)

  /**
   * Finds the first document in the query and updates it.
   * @param query query to match
   * @param fields fields to be returned
   * @param sort sort to apply before picking first document
   * @param remove if true, document found will be removed
   * @param update update to apply
   * @param getNew if true, the updated document is returned, otherwise the old document is returned (or it would be lost forever) [ignored in remove]
   * @param upsert do upsert (insert if document not present)
   * @return the document
   */
  def findAndModify[Qry: SerializableBSONObject, Srt: SerializableBSONObject, Upd: SerializableBSONObject, Flds: SerializableBSONObject](db: String)(collection: String)(
    query: Qry = Document.empty,
    sort: Srt = Document.empty,
    remove: Boolean = false,
    update: Option[Upd] = None,
    getNew: Boolean = false,
    fields: Flds = Document.empty,
    upsert: Boolean = false)(callback: FindAndModifyRequestFuture) = {
    val cmd = OrderedDocument("findandmodify" -> collection,
      "query" -> query,
      "fields" -> fields,
      "sort" -> sort)

    //if (remove && (update.isEmpty || update.get.isEmpty) && !getNew)
    //throw new IllegalArgumentException("Cannot mix update statements or getNew param with 'REMOVE' mode.")

    if (remove) {
      log.debug("FindAndModify 'remove' mode.")
      cmd += "remove" -> true
    } else {
      log.debug("FindAndModify 'modify' mode.  GetNew? %s Upsert? %s", getNew, upsert)
      update.foreach(_up => {
        log.trace("Update spec set. %s", _up)
        // If first key does not start with a $, then the object must be inserted as is and should be checked.
        // TODO - FIX AND UNCOMMENT ME
        //if (_up.filterKeys(k => k.startsWith("$")).isEmpty) checkObject(_up)
        cmd += "update" -> _up
        // TODO - Make sure an error is thrown here that forces its way out.
      })
      cmd += "new" -> getNew
      cmd += "upsert" -> upsert
    }

    implicit val valM = callback.m
    implicit val valDec = new SerializableFindAndModifyResult[callback.T]()(callback.decoder, valM)

    runCommand(db, cmd)(SimpleRequestFutures.command((reply: FindAndModifyResult[callback.T]) => {
      log.trace("Got a result from 'findAndModify' command: %s", reply)
      val doc = reply.value
      if (boolCmdResult(reply, false) && !doc.isEmpty) {
        callback(doc.get.asInstanceOf[callback.T])
      } else {
        callback(reply.getAs[String]("errmsg") match {
          case Some("No matching object found") => new NoMatchingDocumentError()
          case default => {
            log.warning("Command 'findAndModify' may have failed. Bad Reply: %s", reply)
            new MongoException("FindAndModifyError: %s".format(default))
          }
        })
      }
    }))

  }

  def update[Upd](db: String)(collection: String)(query: BSONDocument, update: Upd, upsert: Boolean = false, multi: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, uM: SerializableBSONObject[Upd]) {
    /**
     * If a field block doesn't start with a ($ - special type) we need to validate the keys
     * Since you can't mix $set, etc with a regular "object" this filters safely.
     * TODO - Fix and uncomment!!!!
     */
    // if (update.filterKeys(k => k.startsWith("$")).isEmpty) checkObject(update)
    send(UpdateMessage(db + "." + collection, query, update, upsert, multi), callback)
  }

  def save[T](db: String)(collection: String)(obj: T)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    m.checkObject(obj)
    throw new UnsupportedOperationException("Save doesn't currently function with the new system.")
    /*obj.get("_id") match {
      case Some(id) => {
        id match {
          case oid: ObjectId => oid.notNew()
          case default => {}
        }
        update(db)(collection)(Document("_id" -> id), obj, true, false)(callback)(concern)
      }
      case None => {
        obj += "_id" -> new ObjectId()
        insert(db)(collection)(obj)(callback)(concern)
      }
    }*/
  }

  def remove[T](db: String)(collection: String)(obj: T, removeSingle: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    send(DeleteMessage(db + "." + collection, obj, removeSingle), callback)
  }

  // TODO - FindAndModify / FindAndRemove

  def createIndex[Kys <% BSONDocument, Opts <% BSONDocument](db: String)(collection: String)(keys: Kys, options: Opts = Document.empty)(callback: WriteRequestFuture) {
    implicit val idxSafe = WriteConcern.Safe
    val b = Document.newBuilder
    b += "name" -> indexName(keys)
    b += "ns" -> (db + "." + collection)
    b += "key" -> keys
    b ++= options
    insert(db)("system.indexes")(b.result, validate = false)(callback)
  }

  def createUniqueIndex[Idx <% BSONDocument](db: String)(collection: String)(keys: Idx)(callback: WriteRequestFuture) {
    implicit val idxSafe = WriteConcern.Safe
    createIndex(db)(collection)(keys, Document("unique" -> true))(callback)
  }

  /**
   * NOTE: If you want the "Returns Bool" version of these, use the version on Collection or DB
   */
  def dropAllIndexes(db: String)(collection: String)(callback: SingleDocQueryRequestFuture) {
    dropIndex(db)(collection)("*")(callback)
  }

  /**
   * NOTE: If you want the "Returns Bool" version of these, use the version on Collection or DB
   */
  def dropIndex(db: String)(collection: String)(name: String)(callback: SingleDocQueryRequestFuture) {
    // TODO index cache
    runCommand(db, Document("deleteIndexes" -> (db + "." + collection), "index" -> name))(callback)
  }

  // FIXME this doesn't really mean a whole lot on a connection pool.
  def connected_? = {
    if (checkMasterState.isDefined) {
      true
    } else {
      checkMaster()
      checkMasterState.isDefined
    }
  }

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

  def apply(hostname: String = "localhost", port: Int = 27017) = {
    log.debug("New Connection with hostname '%s', port '%s'", hostname, port)
    // For now, only support Direct Connection

    new DirectConnection(new InetSocketAddress(hostname, port))
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
  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture, _overrideLiveCheck: Boolean = false)(implicit connectionActorHolder: ConnectionActorHolder, concern: WriteConcern): Unit = {
    val connectionActor = connectionActorHolder.actor
    val actorMessage: ConnectionActor.Incoming =
      (msg, f) match {
        case (m: QueryMessage, rf: CursorQueryRequestFuture) =>
          ConnectionActor.SendClientCursorMessage(m, _overrideLiveCheck)
        case (m: GetMoreMessage, rf: GetMoreRequestFuture) =>
          ConnectionActor.SendClientGetMoreMessage(m, _overrideLiveCheck)
        case (m: QueryMessage, rf: SingleDocQueryRequestFuture) =>
          ConnectionActor.SendClientSingleDocumentMessage(m, _overrideLiveCheck)
        case (m: QueryMessage, rf: FindAndModifyRequestFuture) =>
          ConnectionActor.SendClientOptionalSingleDocumentMessage(m, _overrideLiveCheck)
        case (m: MongoClientWriteMessage, rf: WriteRequestFuture) =>
          ConnectionActor.SendClientSingleWriteMessage(m, concern, _overrideLiveCheck)
        case (m: MongoClientWriteMessage, rf: BatchWriteRequestFuture) =>
          ConnectionActor.SendClientBatchWriteMessage(m, concern, _overrideLiveCheck)
        case (m: KillCursorsMessage, NoOpRequestFuture) =>
          ConnectionActor.SendClientKillCursorsMessage(m, _overrideLiveCheck)
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

