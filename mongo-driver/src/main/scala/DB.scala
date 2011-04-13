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

package com.mongodb

import org.bson.util.Logging
import org.bson._
import com.mongodb.futures._
import com.mongodb.wire.QueryMessage


class DB protected[mongodb](val dbName: String)(implicit val connection: MongoConnection) extends Logging {

  def collectionNames(callback: Seq[String] => Unit) {
    val qMsg = QueryMessage("%s.system.namespaces".format(dbName), 0, 0, Document.empty)
    log.debug("[%s] Querying for Collection Names with: %s", dbName, qMsg)
    connection.send(qMsg, RequestFutures.find((cursor: Option[Cursor], res: FutureResult) => {
      log.debug("Got a result from listing collections: %s", cursor)
       //TODO - do we want to add WithFilter, etc? if !doc.getOrElse("$").contains("$")) {
      callback(for {
        doc <- cursor.get
        val name = doc.as[String]("name")
        if !name.contains("$")
      } yield name)
    }))
  }

  /**
   * WARNING: You *must* use an ordered list or commands won't work
   */
  protected[mongodb] def runCommand[A <% BSONDocument](collection: String, cmd: A, f: SingleDocQueryRequestFuture) =
    connection.runCommand("%s.%s".format(dbName, collection), cmd, f)


}
