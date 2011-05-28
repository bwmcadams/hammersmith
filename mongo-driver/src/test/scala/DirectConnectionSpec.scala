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
import futures.RequestFutures
import org.bson.collection._
import org.specs2.time.Time._
import org.bson.util.Logging
import org.bson.types._
import org.specs2.execute.Result
import org.specs2.Specification
import org.specs2.specification._

class DirectConnectionSpec extends Specification with Logging { def is =
  "The MongoDB Direct Connection"                          ^
    "Connect correctly and grab isMaster, then disconnect" ! mongo(connectIsMaster)^
    "Iterate a simple cursor correctly"                    ! mongo(iterateSimpleCursor)^
    "Iterate a complex (iteratee) cursor correctly"        ! mongo(iterateComplexCursor)^
    "Correctly calculate values for 'distinct'"            ! mongo(distinctValue)^
                                                           endp^
  "The write behavior"                                     ^
    "Support 'blind' (NoOp) writes"                        ! mongo(noopInsert)^
    "Support inserts with no (default) write concern"      ! mongo(insertWithDefaultWriteConcern)^
    "Support inserts with implicit safe write concern"     ! mongo(insertWithSafeImplicitWriteConcern)^
                                                           end
    

  trait mongoConn extends AroundOutside[MongoConnection] {

    var conn = MongoConnection()

    def around[T <% Result](t: =>T) = {
      conn.connected_? must eventually(beTrue)
      t
      /*conn.close()
      conn.connected_? must eventually(beFalse)*/
    }

    def outside: MongoConnection = { conn = MongoConnection()
                                     conn }
  }

  object mongo extends mongoConn

  def connectIsMaster(conn: MongoConnection) = {
    conn.databaseNames({ dbs: Seq[String] => dbs.foreach(log.trace("DB: %s", _)) })

    conn("test").collectionNames({ colls: Seq[String] => colls.foreach(log.trace("Collection: %s", _)) })

    conn.connected_? must eventually(beTrue)

  }

  // todo - this relies heavily on whats on my local workstation; needs to be generic
  def iterateSimpleCursor(conn: MongoConnection) = {
    var x = 0
    conn("bookstore").find("inventory")(Document.empty, Document.empty)((cursor: Cursor) => {
      for (doc <- cursor) {
        x += 1
      }
    })

    x must eventually (be_==(336))
  }

  def iterateComplexCursor(conn: MongoConnection) = {
    var x = 0
    conn("bookstore").find("inventory")(Document.empty, Document.empty)((cursor: Cursor) => {
      def next(op: Cursor.IterState): Cursor.IterCmd = op match {
        case Cursor.Entry(doc) => {
          x += 1
          if (x < 100) Cursor.Next(next) else Cursor.Done
        }
        case Cursor.Empty => {
          if (x < 100) Cursor.NextBatch(next) else Cursor.Done
        }
        case Cursor.EOF => {
          Cursor.Done
        }
      }
      Cursor.iterate(cursor)(next)
    })

    x must eventually(5, 5.seconds) (be_==(100))
  }

  def distinctValue(conn: MongoConnection) = {
    conn("bookstore")("inventory").distinct("author")((values: Seq[Any]) => {
      for (item <- values) {
        log.trace("Got a value: %s", item.asInstanceOf[String])
      }
    })

    success
  }

  def noopInsert(conn: MongoConnection) = {
    val mongo = conn("testHammersmith")("test_insert")
    mongo.dropCollection()(success => {
      log.info("Dropped collection... Success? " + success)
    })
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz")){}
    // TODO - Implement 'count'
    var doc: BSONDocument = null
    mongo.findOne(Document("foo" -> "bar"))((_doc: BSONDocument) => {
      doc = _doc 
    })
    doc must not (beNull.eventually)
    doc must eventually (havePairs("foo" -> "bar", "bar" -> "baz"))
  }

  def insertWithDefaultWriteConcern(conn: MongoConnection) = {
    val mongo = conn("testHammersmith")("test_insert")
    mongo.dropCollection()(success => {
      log.info("Dropped collection... Success? " + success)
    })
    var id: AnyRef = null
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))((oid: Option[AnyRef], res: WriteResult) => {
      id = oid.getOrElse(null)
    })
    // TODO - Implement 'count'
    var doc: BSONDocument = null
    mongo.findOne(Document("foo" -> "bar"))((_doc: BSONDocument) => {
      doc = _doc 
    })
    doc must not (beNull.eventually)
    doc must eventually (havePairs("foo" -> "bar", "bar" -> "baz"))
  }

  def insertWithSafeImplicitWriteConcern(conn: MongoConnection) = {
    val mongo = conn("testHammersmith")("test_insert")
    implicit val safeWrite = WriteConcern.Safe
    mongo.dropCollection()(success => {
      log.info("Dropped collection... Success? " + success)
    })
    var id: Option[AnyRef] = null
    var ok: Option[Boolean] = None

    val handler = RequestFutures.write((result: Either[Throwable, (Option[AnyRef], WriteResult)]) => { 
      result match {
        case Right((oid, wr)) => {
          ok = Some(true)
          id = oid        }
        case Left(t) => {
          ok = Some(false)
          log.error(t, "Command Failed.")
        }
      }
    })
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))(handler)
    ok must eventually { beSome(true) }
    id must not (beNull.eventually)
    // TODO - Implement 'count'
    var doc: BSONDocument = null
    mongo.findOne(Document("foo" -> "bar"))((_doc: BSONDocument) => {
      doc = _doc 
    })
    doc must not (beNull.eventually)
    doc must eventually (havePairs("foo" -> "bar", "bar" -> "baz"))
  }

/*    "Support findAndModify" in {
      val mongo = conn("testHammersmith")("test_findModify")
      mongo.insert(Document("x" -> 1), Document("x" -> 2), Document("x" -> 3))

    }*/
}
