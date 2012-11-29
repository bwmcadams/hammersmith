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
import com.mongodb.async.util._
import com.mongodb.async.futures.RequestFutures
import org.bson.collection._
import org.specs2.time.Time._
import org.bson.util.Logging
import org.bson.types._
import org.specs2.execute.Result
import org.specs2.SpecificationWithJUnit
import org.specs2.specification._
import org.specs2.matcher._
import org.junit.runner._
import org.specs2.runner._
import org.specs2.SpecificationWithJUnit

trait HammersmithDefaultDBNames {
  val integrationTestDBName = "hammersmithIntegration"

}

@RunWith(classOf[JUnitRunner])
class DirectConnectionSpec extends SpecificationWithJUnit
    with Logging
    with HammersmithDefaultDBNames {

  def is =
    "The MongoDB Direct Connection" ^
      "Connect correctly and grab isMaster, then disconnect" ! mongo(connectIsMaster _) ^
      endp ^
      "Write Operations" ^
      "Support 'blind' (NoOp) writes" ! mongo(noopInsert _) ^
      "Support inserts with no (default) write concern" ! mongo(insertWithDefaultWriteConcern _) ^
      "Support inserts with implicit safe write concern" ! mongo(insertWithSafeImplicitWriteConcern _) ^
      "Support batch inserts" ! mongo(batchInsert _) ^
      endp ^
      "Read Operations" ^
      "Can count from a collection" ! mongo(countCmd _) ^
      "Iterate a simple cursor correctly" ! mongo(iterateSimpleCursor _) ^
      "Iterate a complex (iteratee) cursor correctly" ! mongo(iterateComplexCursor _) ^
      "Correctly calculate values for 'distinct'" ! mongo(distinctValue _) ^
      "Insert an ObjectId and retrieve it correctly" ! mongo(idDebug _) ^
      endp ^
      "More detailed special commands" ^
      "Support findAndModify" ! mongo(simpleFindAndModify _) ^
      "Support findAndRemove" ! mongo(findAndRemoveTest _) ^
      end
  /*
  trait mongoConn extends AroundOutside[MongoConnection] {

    var conn = MongoConnection()

    def around[T <% Result](t: =>T) = {
      conn.connected_? must eventually(beTrue)
      t
      // TODO - make sure this works (We are reusing)
      [>conn.close()
      conn.connected_? must eventually(beFalse)<]
    }

    def outside: MongoConnection = { conn = MongoConnection()
                                     conn }
  }

  object mongo extends mongoConn*/

  object mongo extends AroundOutside[MongoConnection] {

    val conn = MongoConnection()

    def around[T <% Result](t: ⇒ T) = {
      conn.connected_? must eventually(beTrue)
      t
      // TODO - make sure this works (We are reusing)
      /*conn.close()
      conn.connected_? must eventually(beFalse)*/
    }

    def outside: MongoConnection = conn
  }

  def connectIsMaster(conn: MongoConnection) = {
    conn.databaseNames({ dbs: Seq[String] ⇒ dbs.foreach(log.trace("DB: %s", _)) })

    conn(integrationTestDBName).collectionNames({ colls: Seq[String] ⇒ colls.foreach(log.trace("Collection: %s", _)) })

    conn.connected_? must eventually(beTrue)

  }

  // todo - this relies heavily on whats on my local workstation; needs to be generic
  def iterateSimpleCursor(conn: MongoConnection) = {
    var x = 0
    conn(integrationTestDBName).find("books")(Document.empty, Document.empty)((cursor: Cursor[Document]) ⇒ {
      for (doc ← cursor) {
        x += 1
      }
    })

    x must eventually(be_==(336))
  }

  def iterateComplexCursor(conn: MongoConnection) = {
    var x = 0
    conn(integrationTestDBName).find("books")(Document.empty, Document.empty)((cursor: Cursor[Document]) ⇒ {
        def next(op: Cursor.IterState): Cursor.IterCmd = op match {
          case Cursor.Entry(doc) ⇒ {
            x += 1
            if (x < 100) Cursor.Next(next) else Cursor.Done
          }
          case Cursor.Empty ⇒ {
            if (x < 100) Cursor.NextBatch(next) else Cursor.Done
          }
          case Cursor.EOF ⇒ {
            Cursor.Done
          }
        }
      Cursor.iterate(cursor)(next)
    })

    x must eventually(5, 5.seconds)(be_==(100))
  }

  def distinctValue(conn: MongoConnection) = {
    conn(integrationTestDBName)("books").distinct("author")((values: Seq[Any]) ⇒ {
      for (item ← values) {
        log.trace("Got a value: %s", item.asInstanceOf[String])
      }
    })

    success
  }

  def noopInsert(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("test_insert")
    mongo.dropCollection()(success ⇒ {
      log.debug("Dropped collection... Success? " + success)
    })
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz")) {}
    // TODO - Implement 'count'
    var doc: Document = Document.empty
    mongo.findOne(Document("foo" -> "bar"))((_doc: Document) ⇒ {
      doc = _doc
    })
    eventually(doc must havePairs("foo" -> "bar", "bar" -> "baz"))
  }

  def insertWithDefaultWriteConcern(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("test_insert")
    mongo.dropCollection()(success ⇒ {
      log.info("Dropped collection... Success? " + success)
    })
    var id: AnyRef = null
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))((oid: Option[AnyRef], res: WriteResult) ⇒ {
      id = oid.getOrElse(null)
    })
    // TODO - Implement 'count'
    var doc: Document = Document.empty
    mongo.findOne(Document("foo" -> "bar"))((_doc: Document) ⇒ {
      doc = _doc
    })
    eventually(doc must havePairs("foo" -> "bar", "bar" -> "baz"))
  }

  def insertWithSafeImplicitWriteConcern(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("test_insert")
    implicit val safeWrite = WriteConcern.Safe
    mongo.dropCollection()(success ⇒ {
      log.debug("Dropped collection... Success? " + success)
    })
    var id: Option[AnyRef] = null
    var ok: Option[Boolean] = None

    val handler = RequestFutures.write((result: Either[Throwable, (Option[AnyRef], WriteResult)]) ⇒ {
      result match {
        case Right((oid, wr)) ⇒ {
          ok = Some(true)
          id = oid
        }
        case Left(t) ⇒ {
          ok = Some(false)
          log.error(t, "Command Failed.")
        }
      }
    })
    mongo.insert(Document("foo" -> "bar", "bar" -> "baz"))(handler)
    ok must eventually { beSome(true) }
    id must not(beNull.eventually)
    // TODO - Implement 'count'
    var doc: Document = null
    mongo.findOne(Document("foo" -> "bar"))((_doc: Document) ⇒ {
      doc = _doc
    })
    doc must not(beNull.eventually)
    eventually(doc must havePairs("foo" -> "bar", "bar" -> "baz"))
  }

  def idDebug(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("idGen")
    mongo.dropCollection()(success ⇒ {
      log.debug("Dropped collection... Success? " + success)
    })
    val id = new ObjectId()
    var ok: Option[Boolean] = None
    var insertedID: Option[AnyRef] = None
    val handler = RequestFutures.write((result: Either[Throwable, (Option[AnyRef], WriteResult)]) ⇒ {
      result match {
        case Right((oid, wr)) ⇒ {
          ok = Some(true)
          insertedID = oid
        }
        case Left(t) ⇒ {
          ok = Some(false)
        }
      }
    })
    mongo.insert(Document("_id" -> id, "foo" -> "y", "bar" -> "x"))(handler)

    insertedID must beSome(id) //.EVENTUALLYFUCKYOUSPECS2YOUPIECEOFSHIT //Wait for the insert to finish?

    var savedID: Option[ObjectId] = None
    //TODO - test findOneByID
    mongo.findOne()((_doc: Document) ⇒ {
      savedID = _doc.getAs[ObjectId]("_id")
    })
    savedID must eventually(beSome(id))
  }

  def countCmd(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("countCmd")
    mongo.dropCollection() { success ⇒ }
    var n: Int = -10
    mongo.count()((_n: Int) ⇒ n = _n)
    n must eventually(beEqualTo(0))

    // Now stuff some crap in there to test again
    for (i ← 0 until 10)
      mongo.insert(Document("foo" -> "y", "bar" -> "x")) {}

    mongo.count()((_n: Int) ⇒ n = _n)

    n must eventually(beEqualTo(10))
  }

  def batchInsert(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("batchInsert")
    mongo.dropCollection() { success ⇒ }
    mongo.batchInsert((0 until 100).map(x ⇒ Document("x" -> x)): _*) {}
    var n: Int = -10
    mongo.count()((_n: Int) ⇒ n = _n)
    n must eventually(beEqualTo(100))
  }
  def simpleFindAndModify(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("findModify")
    mongo.dropCollection() { success ⇒ }
    mongo.insert(Document("name" -> "Next promo", "inprogress" -> false, "priority" -> 0, "tasks" -> Seq("select product", "add inventory", "do placement"))) {}
    mongo.insert(Document("name" -> "Biz report", "inprogress" -> false, "priority" -> 1, "tasks" -> Seq("run sales report", "email report"))) {}
    mongo.insert(Document("name" -> "Biz report", "inprogress" -> false, "priority" -> 2, "tasks" -> Seq("run marketing report", "email report"))) {}

    var found: Document = Document.empty
    val startDate = new java.util.Date
    mongo.findAndModify(query = Document("inprogress" -> false, "name" -> "Biz report"),
      sort = Document("priority" -> -1),
      update = Some(Document("$set" -> Document("inprogress" -> true, "started" -> startDate))),
      getNew = true) { doc: Option[Document] ⇒
        doc.foreach(found = _)
      }
    found must eventually(havePairs("inprogress" -> true, "name" -> "Biz report", "started" -> startDate))
  }

  def findAndRemoveTest(conn: MongoConnection) = {
    val mongo = conn(integrationTestDBName)("findRemove")
    mongo.dropCollection() { success ⇒ }
    mongo.batchInsert((0 until 100).map(x ⇒ Document("x" -> x)): _*) {}
    var n: Int = -10
    mongo.count()((_n: Int) ⇒ n = _n)
    n must eventually(beEqualTo(100))
    var x: Int = -1
    mongo.findAndRemove(Document.empty) { result: Option[Document] ⇒
      result match {
        case Some(doc) ⇒ x = doc.as[Int]("x")
      }
    }
    x must eventually(beEqualTo(0))
    mongo.findAndRemove(Document.empty) { result: Option[Document] ⇒
      result match {
        case Some(doc) ⇒ x = doc.as[Int]("x")
      }
    }
    x must eventually(beEqualTo(1))
    mongo.findAndRemove(Document.empty) { result: Option[Document] ⇒
      result match {
        case Some(doc) ⇒ x = doc.as[Int]("x")
      }
    }
    x must eventually(beEqualTo(2))
    success
  }
}
