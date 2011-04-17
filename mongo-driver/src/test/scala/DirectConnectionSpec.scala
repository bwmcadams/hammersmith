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
package com.mongodb.test

import com.mongodb.futures.FutureResult
import com.mongodb.{ Cursor, MongoConnection }
import org.bson.Document
import org.bson.util.{ Logger, Logging }
import org.specs2.mutable._
import org.specs2.runner._
import scala.annotation.tailrec

class DirectConnectionSpec extends SpecificationWithJUnit with Logging {
  //  println(org.apache.commons.logging.Log)

  "The MongoDB Direct Connection" should {
    "Connect correctly and grab isMaster" in {
      val conn = MongoConnection("localhost")

      while (!conn.connected_?) {}
      conn.databaseNames({ dbs: Seq[String] => dbs.foreach(log.info("DB: %s", _)) })
      conn("test").collectionNames({ colls: Seq[String] => colls.foreach(log.info("Collection: %s", _)) })
      // TODO - This highlights the need for a blockable future
      Thread.sleep(2500)

      conn must not beNull
    }
    //    "Iterate a Cursor Correctly" in {
    //      val conn = MongoConnection("localhost")
    //
    //      while (!conn.connected_?) {}
    //      conn("bookstore").find("inventory")(Document.empty, Document.empty)((cursor: Option[Cursor], res: FutureResult) => {
    //        if (res.ok && cursor.isDefined) {
    //          log.debug("Got a result from 'find' command")
    //          Cursor.basicIter(cursor.get) { doc =>
    //            log.info("Got a doc: %s", doc)
    //          }
    //        } else {
    //          log.warning("Find failed: %s / Cursor: %s", res, cursor.get)
    //        }
    //      })
    //      // TODO - This highlights the need for a blockable future
    //      Thread.sleep(7500)
    //
    //      conn must not beNull
    //    }

  }
}