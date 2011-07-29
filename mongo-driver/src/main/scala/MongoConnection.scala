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

