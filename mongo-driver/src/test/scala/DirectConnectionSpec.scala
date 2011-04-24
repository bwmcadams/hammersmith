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
package test

import com.mongodb.async.{ Cursor, MongoConnection }
import org.bson.collection.Document
import org.specs2.mutable._
import org.bson.util.Logging

class DirectConnectionSpec extends SpecificationWithJUnit with Logging {
  //  println(org.apache.commons.logging.Log)

  "The MongoDB Direct Connection" should {
    val conn = MongoConnection("localhost")
    "Connect correctly and grab isMaster" in {


      conn.databaseNames({ dbs: Seq[String] => dbs.foreach(log.trace("DB: %s", _)) })

      conn("test").collectionNames({ colls: Seq[String] => colls.foreach(log.trace("Collection: %s", _)) })


      conn.connected_? must eventually(beTrue)

    }
    // todo - this relies heavily on whats on my local workstation; needs to be generic
    "Iterate a Cursor Correctly" in {
      var x = 0
      conn("bookstore").find("inventory")(Document.empty, Document.empty)((cursor: Cursor) => {
        log.debug("Got a result from 'find' command")
        for (doc <- cursor) {
          x += 1
          log.trace("Got a doc: %s", x)
        }
        log.info("Done iterating, %s results.", x)
      })

      x must eventually (be_==(336))
    }

  }
}