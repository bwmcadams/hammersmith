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
import org.specs2.Specification
import org.specs2.specification._
import org.specs2.matcher._

class MongoURISpec extends Specification with ThrownExpectations with Logging {
  def is =
    "The MongoDB URI Parser" ^ p ^
      "Should function basically with just a DB name" ^
      basicURI1().test ^ endp ^
      "Should function basically with a DB name & collection" ^
      basicURI2().test ^ endp ^
      "Should extract a user/password entry correctly" ^
      userPass().test ^ endp ^
      "Should extract a user/password & port entry correctly" ^
      userPassAndPort().test ^ endp ^
      "Should extract a user/password & multiple hosts w/ port entry correctly" ^
      userPassAndMultiHostWithPort().test ^ endp ^
      end

  abstract class URITest(uri: String) {
    val (hosts, db, collection, login, password) = uri match {
      case MongoURI(h, d, c, l, p, o) => (h, d, c, l, p)
      case default => {
        log.error("Parsing failed.")
        throw new Exception
      }
    }
    val testHost: Seq[(String, Int)] = List.empty[(String, Int)]
    val testDB: Option[String] = None
    val testColl: Option[String] = None
    val testLogin: Option[String] = None
    val testPass: Option[String] = None
    val testOptions: MongoOptions = MongoOptions()

    def test = {
      log.info("HOSTS: %s DB: %s COLL: %s LOGIN: %s PASS: %s", hosts, db, collection, login, password)
      "Have the expected hostname" ! hostChk ^
        "Have the expected db" ! dbChk ^
        "Have the expected collection" ! collChk ^
        "Have the expected login" ! loginChk ^
        "Have the expected password" ! passChk ^
        "Receive the expected URI Parse return types" ! uriConnChk
    }

    def hostChk = hosts must haveTheSameElementsAs(testHost)
    def dbChk = db must_== (testDB)
    def collChk = collection must_== (testColl)
    def loginChk = login must_== (testLogin)
    def passChk = password must_== (testPass)
    def uriConnChk = {
      try {
        val connSet = MongoConnection.fromURI(uri)

        connSet._1 must haveSuperclass[MongoConnection]
        testDB match {
          case Some(dbName) => connSet._2 must beSome.which(_.name == dbName)
          case None => connSet._2 must beNone
        }
        testColl match {
          case Some(collName) => connSet._3 must beSome.which(_.name == collName)
          case None => connSet._3 must beNone
        }
      } catch {
        case use: UnsupportedOperationException => if (hosts.size > 1) {
          success
        } else throw use
        case e => throw e
      }
      success

    }
  }

  case class basicURI1() extends URITest("mongodb://foo/bar") {
    override val testHost = List(("foo", 27017))
    override val testDB = Some("bar")
  }

  case class basicURI2() extends URITest("mongodb://foo/bar.baz") {
    override val testHost = List(("foo", 27017))
    override val testDB = Some("bar")
    override val testColl = Some("baz")
  }

  case class userPass() extends URITest("mongodb://user:pass@host/bar") {
    override val testHost = List(("host", 27017))
    override val testDB = Some("bar")
    override val testLogin = Some("user")
    override val testPass = Some("pass")
  }

  case class userPassAndPort() extends URITest("mongodb://user:pass@host:27017/bar") {
    override val testHost = List(("host", 27017))
    override val testDB = Some("bar")
    override val testLogin = Some("user")
    override val testPass = Some("pass")
  }

  case class userPassAndMultiHostWithPort() extends URITest("mongodb://user:pass@host1:27011,host2:27012,host3:27013/bar") {
    override val testHost = List(("host1", 27011), ("host2", 27012), ("host3", 27013))
    override val testDB = Some("bar")
    override val testLogin = Some("user")
    override val testPass = Some("pass")
  }

}

// vim: set ts=2 sw=2 sts=2 et:
