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
import com.mongodb.async.futures._
import com.mongodb.async.util._
import com.mongodb.async.wire._
import org.bson._
import org.bson.collection._

object Collection extends Logging {

}

class Collection(val name: String)(implicit val db: DB) extends Logging {

  protected[mongodb] def send(msg: MongoClientMessage, f: RequestFuture)(implicit concern: WriteConcern = this.writeConcern) =
    db.send(msg, f)

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   */
  protected[mongodb] def runCommand[Cmd <% BSONDocument](ns: String, cmd: Cmd)(f: SingleDocQueryRequestFuture) {
    val qMsg = MongoConnection.createCommand(ns, cmd)
    log.trace("Created Query Message: %s, id: %d", qMsg, qMsg.requestID)
    send(qMsg, f)
  }

  private val nameWithDB = db.name + "." + name

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   * TODO - Would this perform faster partially applied?
   * TODO - Support Options here
   */
  def command[Cmd <% BSONDocument](cmd: Cmd)(f: SingleDocQueryRequestFuture) {
    db.command(cmd)(f)
  }

  def command(cmd: String): SingleDocQueryRequestFuture => Unit =
    command(Document(cmd -> 1))_

  /**
   * Repeated deliberately enough times that i'll notice it later.
   * Document all methods esp. find/findOne and special ns versions
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * TODO - SCALADOC
   * for (i <- 1 to 5000) println("TODO - SCALADOC")
   */
  def find[Qry <: BSONDocument, Flds <: BSONDocument](query: Qry = Document.empty, fields: Flds = Document.empty, numToSkip: Int = 0, batchSize: Int = 0)(callback: CursorQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    val qMsg = QueryMessage(nameWithDB, numToSkip, batchSize, query, fieldSpec(fields))
    send(qMsg, callback)
  }

  def findOne[Qry <: BSONDocument, Flds <: BSONDocument](query: Qry = Document.empty, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    val qMsg = QueryMessage(nameWithDB, 0, -1, query, fieldSpec(fields))
    send(qMsg, callback)
  }

  // TODO - should we allow any and do boxing elsewhere?
  // TODO - FindOne is Option[] returning, ensure!
  def findOneByID[Id <: AnyRef, Flds <: BSONDocument](id: Id, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    findOne(Document("_id" -> id), fields)(callback)
  }

  // TODO - Immutable mode / support immutable objects
  def insert[T](doc: T, validate: Boolean = true)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    log.trace("Inserting: %s to %s.%s with WriteConcern: %s", doc, db, name, concern)
    val checked = if (validate) {
      m.checkObject(doc)
      m.checkID(doc)
    } else {
      log.debug("Validation of objects disabled; no ID Gen.")
      doc
    }
    send(InsertMessage(nameWithDB, checked), callback)
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
  def batchInsert[T](docs: T*)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    log.trace("Batch Inserting: %s to %s.%s with WriteConcern: %s", docs, db, name, concern)
    val checked = docs.map(x => {
      m.checkObject(x)
      m.checkID(x)
    })
    send(InsertMessage(nameWithDB, checked: _*), callback)
  }

  def update[Upd](query: BSONDocument, update: Upd, upsert: Boolean = false, multi: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, uM: SerializableBSONObject[Upd]) {
    /**
     * If a field block doesn't start with a ($ - special type) we need to validate the keys
     * Since you can't mix $set, etc with a regular "object" this filters safely.
     * TODO - Fix and uncomment!!!!
     */
    // if (update.filterKeys(k => k.startsWith("$")).isEmpty) checkObject(update)
    send(UpdateMessage(nameWithDB, query, update, upsert, multi), callback)
  }

  def save[T](obj: T)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    m.checkObject(obj)
    throw new UnsupportedOperationException("Save doesn't currently function with the new system.")
    /*obj.get("_id") match {
      case Some(id) => {
        id match {
          case oid: ObjectId => oid.notNew()
          case default => {}
        }
        update(Document("_id" -> id), obj, true, false)(callback)(concern)
      }
      case None => {
        obj += "_id" -> new ObjectId()
        insert(db)(collection)(obj)(callback)(concern)
      }
    }*/
  }

  def remove[T](obj: T, removeSingle: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    send(DeleteMessage(nameWithDB, obj, removeSingle), callback)
  }

  // TODO - FindAndModify / FindAndRemove

  def createIndex[Kys <% BSONDocument, Opts <% BSONDocument](keys: Kys, options: Opts = Document.empty)(callback: WriteRequestFuture) {
    implicit val idxSafe = WriteConcern.Safe
    val b = Document.newBuilder
    b += "name" -> indexName(keys)
    b += "ns" -> (nameWithDB)
    b += "key" -> keys
    b ++= options
    db("system.indexes").insert(b.result, validate = false)(callback)
  }

  def createUniqueIndex[Idx <% BSONDocument](keys: Idx)(callback: WriteRequestFuture) {
    implicit val idxSafe = WriteConcern.Safe
    createIndex(keys, Document("unique" -> true))(callback)
  }

  private def dropAllIndexesRequestFuture(callback: SingleDocQueryRequestFuture) {
    dropIndexRequestFuture("*")(callback)
  }

  private def dropIndexRequestFuture(idxName: String)(callback: SingleDocQueryRequestFuture) {
    // TODO index cache
    runCommand(db.name, Document("deleteIndexes" -> (nameWithDB), "index" -> idxName))(callback)
  }

  def dropAllIndexes()(callback: (Boolean) => Unit) {
    dropAllIndexesRequestFuture(boolCmdResultCallback(callback))
  }

  def dropIndex(idxName: String)(callback: (Boolean) => Unit) {
    dropIndexRequestFuture(idxName)(boolCmdResultCallback(callback))
  }

  // TODO - dropIndex(keys)

  def dropCollection()(callback: (Boolean) => Unit) {
    // TODO - Reset Index Cache
    command(Document("drop" -> name))(boolCmdResultCallback(callback))
  }

  /**
   * Counts the number of documents in a given namespace
   * -1 indicates an error, for now
   */
  def count[Qry: SerializableBSONObject, Flds: SerializableBSONObject](query: Qry = Document.empty,
    fields: Flds = Document.empty,
    limit: Long = 0,
    skip: Long = 0)(callback: Int => Unit) = {
    val builder = OrderedDocument.newBuilder
    builder += ("count" -> name)
    builder += ("query" -> query)
    builder += ("fields" -> fields)
    if (limit > 0)
      builder += ("limit" -> limit)
    if (skip > 0)
      builder += ("skip" -> skip)
    runCommand(db.name, builder.result)(SimpleRequestFutures.command((doc: Document) => {
      log.trace("Got a result from 'count' command: %s", doc)
      callback(doc.getAsOrElse[Double]("n", -1.0).toInt)
    }))
  }

  // TODO - Rename

  // TODO - Group

  // TODO - MapReduce

  // TODO - getIndexInfo

  /**
   * Calls findAndModify in remove only mode with
   * fields={}, sort={}, remove=true, getNew=false, upsert=false
   * @param query
   * @return the removed document
   */
  def findAndRemove[Qry: SerializableBSONObject](query: Qry = Document.empty)(callback: FindAndModifyRequestFuture) =
    findAndModify(query = query, remove = true, update = Option[Document](null))(callback)

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
  def findAndModify[Qry: SerializableBSONObject, Srt: SerializableBSONObject, Upd: SerializableBSONObject, Flds: SerializableBSONObject](
    query: Qry = Document.empty,
    sort: Srt = Document.empty,
    remove: Boolean = false,
    update: Option[Upd] = None,
    getNew: Boolean = false,
    fields: Flds = Document.empty,
    upsert: Boolean = false)(callback: FindAndModifyRequestFuture) = {
    val cmd = OrderedDocument("findandmodify" -> name,
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

    runCommand(db.name, cmd)(SimpleRequestFutures.command((reply: FindAndModifyResult[callback.T]) => {
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

  /**
   *
   */
  def distinct[Qry: SerializableBSONObject](key: String, query: Qry = Document.empty)(callback: Seq[Any] => Unit) {
    command(OrderedDocument("distinct" -> name, "key" -> key, "query" -> query))(SimpleRequestFutures.findOne((doc: Document) => callback(doc.getAsOrElse[BSONList]("values", BSONList.empty).asList)))
  }

  /**
   * Defaults to grabbing the DBs setting ( which defaults to the Connection's)  unless we set a specific write concern
   * here.
   */
  protected[mongodb] var _writeConcern: Option[WriteConcern] = None

  /**
   *
   * Set the write concern for this database.
   * Will be used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * Defaults to grabbing the DBs setting ( which defaults to the Connection's)  unless we set a specific write concern
   * here.
   *
   * @param concern (WriteConcern) The write concern to use
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/monggaodb-single-server-data-durability-guide/
   */
  def writeConcern_=(concern: WriteConcern) = _writeConcern = Some(concern)

  /**
   *
   * get the write concern for this database,
   * which is used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * Defaults to grabbing the Connections setting unless we set a specific write concern
   * here.
   *
   * @see WriteConcern
   * @see http://www.thebuzzmedia.com/mongodb-single-server-data-durability-guide/
   */
  def writeConcern = _writeConcern.getOrElse(db.writeConcern)

  def ns = nameWithDB

}
