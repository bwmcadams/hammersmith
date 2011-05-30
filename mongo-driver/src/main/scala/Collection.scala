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
import org.bson.collection._

object Collection extends Logging {

}

class Collection(val name: String)(implicit val db: DB) extends Logging {

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   * TODO - Would this perform faster partially applied?
   * TODO - Support Options here
   */
  def command[A <% BSONDocument](cmd: A)(f: SingleDocQueryRequestFuture) {
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
  /** Note - I tried doing this as a partially applied but the type signature is VERY Unclear to the user - BWM */
  def find(query: BSONDocument = Document.empty, fields: BSONDocument = Document.empty, numToSkip: Int = 0, batchSize: Int = 0)(callback: CursorQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.find(name)(query, fields, numToSkip, batchSize)(callback)
  }

  /** Note - I tried doing this as a partially applied but the type signature is VERY Unclear to the user - BWM  */
  def findOne(query: BSONDocument = Document.empty, fields: BSONDocument = Document.empty)(callback: SingleDocQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.findOne(name)(query, fields)(callback)
  }

  def findOneByID[A <: AnyRef](id: A)(callback: SingleDocQueryRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.findOneByID(name)(id)(callback)
  }

  def insert(doc: BSONDocument, validate: Boolean = true)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
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
  def batchInsert(docs: BSONDocument*)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.batchInsert(name)(docs: _*)(callback)
  }

  def update(query: BSONDocument, update: BSONDocument, upsert: Boolean = false, multi: Boolean = false)
            (callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.update(name)(query, update, upsert, multi)(callback)
  }

  def save(obj: BSONDocument)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.save(name)(obj)(callback)
  }

  def remove(obj: BSONDocument, removeSingle: Boolean = false)
            (callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern) {
    db.remove(name)(obj, removeSingle)(callback)
  }

  // TODO - FindAndModify / FindAndRemove

  def createIndex[A <% BSONDocument, B <% BSONDocument](keys: A, options: B = Document.empty)(callback: WriteRequestFuture) {
    db.createIndex(name)(keys, options)(callback)
  }

  def createUniqueIndex[A <% BSONDocument](keys: A)(callback: WriteRequestFuture) {
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
    command(Document("drop" -> name))(boolCmdResultCallback(callback))
  }

  // TODO - Impl of Count ?

  // TODO - Rename

  // TODO - Group


  // TODO - MapReduce

  // TODO - getIndexInfo

  /**
   *
   */
  def distinct(key: String, query: BSONDocument = Document.empty)(callback: Seq[Any] => Unit) {
    command(OrderedDocument("distinct" -> name, "key" -> key, "query" -> query))(SimpleRequestFutures.findOne((doc: BSONDocument) => callback(doc.getAsOrElse[BSONList]("values", BSONList.empty).asList)))
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
