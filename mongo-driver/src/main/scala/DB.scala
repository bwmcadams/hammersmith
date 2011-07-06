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
import org.bson._
import org.bson.collection._
import com.mongodb.async.futures._
import java.io.{ IOException, ByteArrayOutputStream }
import java.security.MessageDigest
import com.mongodb.async.wire.{InsertMessage , QueryMessage}

class DB(val name: String)(implicit val connection: MongoConnection) extends Logging {

  // TODO - Implement as well as supporting "getCollectionFromString" from the Java driver
  def apply(collection: String) = new Collection(collection)(this)

  // def addUser(username: String, password: String)(f: )
  // def removeUser

  def authenticate(username: String, password: String)(callback: DB => Unit) {
    require(username != null, "Username cannot be null.")
    require(password != null, "Password cannot be null.")
    assume(!authenticated_?, "Already authenticated.")
    val hash = hashPassword(username, password)
    log.debug("Hashed Password: '%s'", hash)
    command[Document]("getnonce")(RequestFutures.findOne((result: Either[Throwable, Document]) => result match {
      // TODO - Callback on failure
      case Right(doc) =>
        doc.getAsOrElse[Int]("ok", 0) match {
          case 1 => {
            val nonce = doc.as[String]("nonce")
            log.debug("Got Nonce: '%s'", nonce)
            val authCmd = OrderedDocument("authenticate" -> 1,
              "user" -> username,
              "nonce" -> nonce,
              "key" -> hexMD5(nonce + username + hash))
            log.debug("Auth Command: %s", authCmd)

            command(authCmd)(RequestFutures.findOne((result: Either[Throwable, Document]) => {
              result match {
                case Right(_doc) =>
                  _doc.getAsOrElse[Int]("ok", 0) match {
                    case 1 => {
                      log.debug("Authenticate succeeded.")
                      login = Some(username)
                      authHash = Some(hash)
                    }
                    case other => log.error("Authentication Failed. '%d' OK status. %s", other, _doc)
                  }
                case Left(e) =>
                  log.error(e, "Authentication Failed.")
                  callback(this)
              }
            }))
          }
          case other => log.error("Failed to get nonce: %s (OK: %s)", doc, other)
        }
      case Left(e) => log.error(e, "Failed to get nonce.")
    }))
  }

  protected[mongodb] def hashPassword(username: String, password: String) = {
    val b = new ByteArrayOutputStream(username.length + 20 + password.length)
    try {
      b.write(username.getBytes)
      b.write(":mongo:".getBytes)
      for (i <- 1 to password.length) {
        // todo there has to be a more efficient way to check this
        assume(password(i) < 128, "Cannot currently support non-ascii passwords.")
        b.write(password(i))
      }

    } catch {
      case ioE: IOException => throw new Exception("Unable to hash Password.", ioE)
    }

    hexMD5(b.toByteArray)
  }

  protected[mongodb] def hexMD5(str: String): String = hexMD5(str.getBytes)
  protected[mongodb] def hexMD5(bytes: Array[Byte]) = {
    md5.reset()
    md5.update(bytes)
    md5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  // TODO Fix me
  def authenticated_? = login.isDefined && authHash.isDefined

  def collectionNames(callback: Seq[String] => Unit) {
    val qMsg = QueryMessage("%s.system.namespaces".format(name), 0, 0, Document.empty)
    log.debug("[%s] Querying for Collection Names with: %s", name, qMsg)
    connection.send(qMsg, SimpleRequestFutures.find((cursor: Cursor[Document]) => {
      log.debug("Got a result from listing collections: %s", cursor)
      val b = Seq.newBuilder[String]

      Cursor.basicIter(cursor) { doc =>
        val n = doc.as[String]("name")
        if (!n.contains("$")) b += n.split(name + "\\.")(1)
      }

      callback(b.result())
    }))
  }

  /**
   * Creates a collection with a given name and options.
   * If the collection does not exist, a new collection is created.
   * Note that if the options parameter is null,
   * the creation will be deferred to when the collection is written to.
   * Possible options:
   * <dl>
   * <dt>capped</dt><dd><i>boolean</i>: if the collection is capped</dd>
   * <dt>size</dt><dd><i>int</i>: collection size (in bytes)</dd>
   * <dt>max</dt><dd><i>int</i>: max number of documents</dd>
   * </dl>
   * @param name the name of the collection to return
   * @param options options
   *
   * The callback will be invoked, when the collection is created, with an instance of the new collection.
   */
  def createCollection[Opts : SerializableBSONObject](name: String, options: Opts)(callback: Collection => Unit) = {
    // TODO - Implement me
    throw new UnsupportedOperationException("Not implemented.")
  }

  /**
   * Evaluates a JavaScript function on the server
   */
  //def eval(code: String, args: Any*)
  /**
   * WARNING: You *must* use an ordered list or commands won't work
   * TODO - Would this perform faster partially applied?
   * TODO - Support Options here
   */
  def command[Cmd <% BSONDocument, Result : SerializableBSONObject : Manifest](cmd: Cmd)(f: SingleDocQueryRequestFuture[Result]): Unit = {
    connection.runCommand(name, cmd)(f)
  }

  def command[Result : SerializableBSONObject : Manifest](cmd: String)(f : SingleDocQueryRequestFuture[Result]) : Unit =
    command(Document(cmd -> 1))(f)

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
  def find[Qry <: BSONDocument, Flds <: BSONDocument, Result](collection: String)(query: Qry = Document.empty, fields: Flds = Document.empty, numToSkip: Int = 0, batchSize: Int = 0)(callback: CursorQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    connection.find(name)(collection)(query, fields, numToSkip, batchSize)(callback)
  }

  /** Note - I tried doing this as a partially applied but the type signature is VERY Unclear to the user - BWM  */
  def findOne[Qry <: BSONDocument, Flds <: BSONDocument, Result](collection: String)(query: Qry = Document.empty, fields: Flds = Document.empty)(callback: SingleDocQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    connection.findOne(name)(collection)(query, fields)(callback)
  }

  def findOneByID[A <: AnyRef, Flds <: BSONDocument, Result](collection: String)(id: A, fields : Flds = Document.empty)(callback: SingleDocQueryRequestFuture[Result])(implicit concern: WriteConcern = this.writeConcern, serializable : SerializableBSONObject[Result]) {
    connection.findOneByID(name)(collection)(id, fields)(callback)
  }

  def insert[T](collection: String)(doc: T, validate: Boolean = true)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T], mf : Manifest[T]) {
    connection.insert(name)(collection)(doc, validate)(callback)
  }

  /**
   * Insert multiple documents at once.
   * Keep in mind, that WriteConcern behavior may be wonky if you do a batchInsert
   * I believe the behavior of MongoDB will cause getLastError to indicate the LAST error 
   * on your batch ---- not the first, or all of them.
   *
   * The WriteRequest used here returns a Seq[] of every generated ID, not a single ID
   */
  def batchInsert[T](collection: String)(docs: T*)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T], mf: Manifest[T]) {
    connection.batchInsert(name)(collection)(docs: _*)(callback)
  }

  def update[Upd](collection: String)(query: BSONDocument, update: Upd, upsert: Boolean = false, multi: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, uM: SerializableBSONObject[Upd]) {
    connection.update(name)(collection)(query, update, upsert, multi)(callback)
  }

  def save[T](collection: String)(obj: T)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T]) {
    connection.save(name)(collection)(obj)(callback)
  }

  def remove[T](collection: String)(obj: T, removeSingle: Boolean = false)(callback: WriteRequestFuture)(implicit concern: WriteConcern = this.writeConcern, m: SerializableBSONObject[T], mf: Manifest[T]) {
    connection.remove(name)(collection)(obj, removeSingle)(callback)
  }

  // TODO - FindAndModify / FindAndRemove

  def createIndex[Kys <% BSONDocument, Opts <% BSONDocument](collection: String)(keys: Kys, options: Opts = Document.empty)(callback: WriteRequestFuture) {
    connection.createIndex(name)(collection)(keys, options)(callback)
  }

  def createUniqueIndex[Idx <% BSONDocument](collection: String)(keys: Idx)(callback: WriteRequestFuture) {
    connection.createUniqueIndex(name)(collection)(keys)(callback)
  }

  def dropAllIndexes(collection: String)(callback: (Boolean) => Unit) {
    connection.dropAllIndexes(name)(collection)(boolCmdResultCallback[Document](callback))
  }

  def dropIndex(collection: String)(idxName: String)(callback: (Boolean) => Unit) {
    connection.dropIndex(name)(collection)(idxName)(boolCmdResultCallback[Document](callback))
  }

  /**
   * Drops the database completely, removing all data from disk.
   * *** USE WITH CAUTION ***
   * Not called drop() as that would conflict with an existing expected Scala method
   * TODO - Ensure getLastError on this?
   * TODO - Remove from any cached db listings
   */
  def dropDatabase()(callback: (Boolean) => Unit) = command[Document]("dropDatabase")(boolCmdResultCallback[Document](callback))

  /**
   * invokes the 'dbStats' command
   */
  def stats() = command[Document]("dbstats")(ignoredResultCallback)

  /**
   * TODO - This is done the same way as the Java Driver's but rather inefficient
   * in that it iterates all of system.namespaces... might be better to findOne
   */
  def collectionExists(name: String)(callback: Boolean => Unit) = collectionNames({ colls: Seq[String] =>
    callback(colls.contains(name))
  })

  def count[Qry : SerializableBSONObject, Flds : SerializableBSONObject](collection: String)(query : Qry = Document.empty,
      fields : Flds = Document.empty,
      limit : Long = 0,
      skip : Long = 0)(callback: Int => Unit) =
    connection.count(name)(collection)(query, fields, limit, skip)(callback)

  // TODO - We can't allow free form getLastError due to the async nature.. it must be locked to the call


  /**
   * Calls findAndModify in remove only mode with
   * fields={}, sort={}, remove=true, getNew=false, upsert=false
   * @param query
   * @return the removed document
   */
  def findAndRemove[Qry : SerializableBSONObject, Result : SerializableBSONObject : Manifest](collection: String)(query: Qry = Document.empty)(callback: SingleDocQueryRequestFuture[Result]) = connection.findAndRemove(name)(collection)(query)(callback)

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
  def findAndModify[Qry : SerializableBSONObject, Srt : SerializableBSONObject, Upd : SerializableBSONObject, Flds : SerializableBSONObject, Result : SerializableBSONObject : Manifest](collection: String)(
                    query: Qry = Document.empty,
                    sort: Srt = Document.empty,
                    remove: Boolean = false,
                    update: Option[Upd] = None,
                    getNew: Boolean = false,
                    fields: Flds = Document.empty,
                    upsert: Boolean = false)(callback: SingleDocQueryRequestFuture[Result]) =
    connection.findAndModify(name)(collection)(query, sort, remove, update, getNew, fields, upsert)(callback)


  /**
   * Gets another database on the same server (without having to go up to connection)
   * @param name Name of the database
   * No serverside op needed so doesn't have to callback
   */
  def sisterDB(name: String) = connection(name)

  // TODO - Slave OK
  /**
   * Defaults to grabbing the Connections setting unless we set a specific write concern
   * here.
   */
  protected[mongodb] var _writeConcern: Option[WriteConcern] = None

  /**
   *
   * Set the write concern for this database.
   * Will be used for writes to any collection in this database.
   * See the documentation for {@link WriteConcern} for more info.
   *
   * Defaults to grabbing the Connections setting unless we set a specific write concern
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
  def writeConcern = _writeConcern.getOrElse(connection.writeConcern)

  override def toString = name

  private val md5 = MessageDigest.getInstance("MD5")
  protected var login: Option[String] = None
  protected var authHash: Option[String] = None

}
