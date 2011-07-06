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
import org.bson._
import org.bson.collection._

object Collection extends Logging {

}

class Collection(val name: String)(implicit val db: DB) extends Logging {

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   * TODO - Would this perform faster partially applied?
   * TODO - Support Options here
   */
  def command[Cmd <% BSONDocument, Result : SerializableBSONObject](cmd: Cmd)(f: SingleDocQueryRequestFuture[Result]) : Unit = {
    db.command[Cmd, Result](cmd)(f)
  }

  def command[Result : SerializableBSONObject](cmd: String)(f : SingleDocQueryRequestFuture[Result]) : Unit =
    command[Document, Result](Document(cmd -> 1))(f)

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
  /** Note - I tried doing this as a partially applied but the type signature is VERY Unclear to the user - BWM */
  def find[Qry <: BSONDocument, Flds <: BSONDocument, Result](query: Qry = Document.empty, fields: Flds = Document.empty, numToSkip: Int = 0, batchSize: Int = 0)(callback: CursorQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    db.find(name)(query, fields, numToSkip, batchSize)(callback)
  }

  /** Note - I tried doing this as a partially applied but the type signature is VERY Unclear to the user - BWM  */
  def findOne[Qry <: BSONDocument, Flds <: BSONDocument, Result](query: Qry = Document.empty, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    db.findOne(name)(query, fields)(callback)
  }

  def findOneByID[Id <: AnyRef, Flds <: BSONDocument, Result](id: Id, fields : Flds = Document.empty)(callback: SingleDocQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    db.findOneByID(name)(id, fields)(callback)
  }

  def insert[T](doc: T, validate: Boolean = true)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    db.insert(name)(doc, validate)(callback)
  }
  
  /**
   * Insert multiple documents at once.
   * Keep in mind, that WriteConcern behavior may be wonky if you do a batchInsert
   * I believe the behavior of MongoDB will cause getLastError to indicate the LAST error 
   * on your batch ---- not the first, or all of them.
   *
   * The WriteRequest used here returns a Seq[] of every generated ID, not a single ID
   */
  def batchInsert[T](docs: T*)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    db.batchInsert(name)(docs: _*)(callback)
  }

  def update[Upd](query: BSONDocument, update: Upd, upsert: Boolean = false, multi: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, uM: SerializableBSONObject[Upd]) {
    db.update(name)(query, update, upsert, multi)(callback)
  }

  def save[T](obj: T)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    db.save(name)(obj)(callback)
  }

  def remove[T](obj: T, removeSingle: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    db.remove(name)(obj, removeSingle)(callback)
  }


  def createIndex[Kys <% BSONDocument, Opts <% BSONDocument](keys: Kys, options: Opts = Document.empty)(callback: WriteRequestFuture) {
    db.createIndex(name)(keys, options)(callback)
  }

  def createUniqueIndex[Idx <% BSONDocument](keys: Idx)(callback: WriteRequestFuture) {
    db.createUniqueIndex(name)(keys)(callback)
  }

  def dropAllIndexes()(callback: (Boolean) => Unit) {
    db.dropAllIndexes(name)(callback)
  }

  def dropIndex(idxName: String)(callback: (Boolean) => Unit) {
    db.dropIndex(name)(idxName)(callback)
  }

  // TODO - dropIndex(keys)

  def dropCollection()(callback: (Boolean) => Unit) {
    // TODO - Reset Index Cache
    command[Document, Document](Document("drop" -> name))(boolCmdResultCallback(callback))
  }

  /**
   * Counts the number of documents in a given namespace
   * -1 indicates an error, for now
   */
  def count[Qry : SerializableBSONObject, Flds : SerializableBSONObject](query : Qry = Document.empty,
      fields : Flds = Document.empty,
      limit : Long = 0,
      skip : Long = 0)(callback: Int => Unit) =
        db.count(name)(query, fields, limit, skip)(callback)

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
  def findAndRemove[Qry : SerializableBSONObject, Result : SerializableBSONObject](query: Qry = Document.empty)(callback: SingleDocQueryRequestFuture[Result]) = db.findAndRemove(name)(query)(callback)

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
  def findAndModify[Qry : SerializableBSONObject, Srt : SerializableBSONObject, Upd : SerializableBSONObject, Flds : SerializableBSONObject, Result : SerializableBSONObject](
                    query: Qry = Document.empty,
                    sort: Srt = Document.empty,
                    remove: Boolean = false,
                    update: Option[Upd] = Option[Document](null),
                    getNew: Boolean = false,
                    fields: Flds = Document.empty,
                    upsert: Boolean = false)(callback: SingleDocQueryRequestFuture[Result]) =
    db.findAndModify(name)(query, sort, remove, update, getNew, fields, upsert)(callback)


  /**
   *
   */
  def distinct[Qry : SerializableBSONObject](key: String, query: Qry = Document.empty)(callback: Seq[Any] => Unit) {
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

  def ns = "%s.%s".format(db.name, name)

}
