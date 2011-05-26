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

import com.mongodb.async._
import org.bson.collection.Document
import org.specs2.mutable._
import org.specs2.time.Time._
import org.bson.util.Logging


class DirectConnectionSpec extends Specification with Logging {
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
    "Iterate a Cursor Correctly" in {
      var x = 0
      conn("bookstore").find("inventory")(Document.empty, Document.empty)((cursor: Cursor) => {
        log.debug("Got a result from 'find' command")
        log.info("Done iterating, %s results.", x)
        def next(op: Cursor.IterState): Cursor.IterCmd = op match {
          case Cursor.Entry(doc) => {
            x += 1
            if (x < 100) Cursor.Next(next) else Cursor.Done
          }
          case Cursor.Empty => {
            log.trace("Empty... Next batch.")
            if (x < 100) Cursor.NextBatch(next) else Cursor.Done
          }
          case Cursor.EOF => {
            log.info("EOF... Cursor done.")
            Cursor.Done
          }
        }
        Cursor.iterate(cursor)(next)
      })

      x must eventually(5, 5.seconds) (be_==(100))
    }
    "Support Distinct" in {
      conn("bookstore")("inventory").distinct("author")((values: Seq[Any]) => {
        for (item <- values) {
          log.trace("Got a value: %s", item.asInstanceOf[String])
        }
      })

      conn must not beNull
    }

    "Support inserts with no (default) write concern" in {
      val mongo = conn("testHammersmith")("test_insert")
      mongo.dropCollection()(success => {
        log.info("Dropped collection... Success? " + success)
      })
      var id: AnyRef = null
      mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))((oid: Option[AnyRef], res: WriteResult) => {
        log.info("Result ID: %s Res: %s", oid, res)
        id = oid.getOrElse(null)
      })
      id must not beNull
    }
    "Support inserts with implicit safe write concern" in {
      val mongo = conn("testHammersmith")("test_insert")
      implicit val safeWrite = WriteConcern.Safe
      mongo.dropCollection()(success => {
        log.info("Dropped collection... Success? " + success)
      })
      var id: AnyRef = null
      mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))((oid: Option[AnyRef], res: WriteResult) => {
        log.info("Result ID: %s Res: %s", oid, res)
        id = oid.getOrElse(null)
      })
      id must not beNull
    }

  }
}