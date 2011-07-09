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

import scala.concurrent.ops._
import com.mongodb.async._
import futures.RequestFutures
import org.bson.collection._
import org.specs2.time.Time._
import org.bson.util.Logging
import org.bson.types._
import org.specs2.execute.Result
import org.specs2.Specification
import org.specs2.specification._
import org.specs2.matcher._
import com.twitter.util.Time

class ConcurrencyTestingSpec extends Specification
  with Logging {
  def is =
    "The MongoDB Direct Connection" ^
      "Works concurrently" ^
      "Support lots of concurrent batch inserts" ! mongo(batchInsert) ^
      end
  object mongo extends AroundOutside[MongoConnection] {

    val conn = MongoConnection()

    def around[T <% Result](t: => T) = {
      conn.connected_? must eventually(beTrue)
      t
      // TODO - make sure this works (We are reusing)
      /*conn.close()
      conn.connected_? must eventually(beFalse)*/
    }

    def outside: MongoConnection = conn
  }

  def batchInsert(conn: MongoConnection) = {
    val mongo = conn("testHammersmith")("batchConcurrencyInsert")
    mongo.dropCollection() { success => }
    mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {}
    var n: Int = -10
    spawn { mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {} }
    spawn { mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {} }
    spawn { mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {} }
    spawn { mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {} }
    spawn { mongo.batchInsert((0 until 100).map(x => Document("x" -> x)): _*) {} }
    var x: Int = 0
    var start = Time.now
    while (x < 10 && n != 600) {
      mongo.count()((_n: Int) => n = _n)
      x += 1
      Thread.sleep(5.seconds.inMillis)
    }
    log.info("Insert tie-out took %s milliseconds", start.untilNow.inMillis)
    n must eventually(beEqualTo(600))
  }
}
