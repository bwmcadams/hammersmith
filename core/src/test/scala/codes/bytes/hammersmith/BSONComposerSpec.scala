/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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

package codes.bytes.hammersmith

import com.typesafe.scalalogging.StrictLogging
import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner

import org.specs2.matcher.ThrownExpectations
import codes.bytes.hammersmith.collection.immutable.{OrderedDocument, DBList, Document}
import codes.bytes.hammersmith.bson._
import java.util.regex.Pattern
import codes.bytes.hammersmith.bson.BSONTimestamp
import codes.bytes.hammersmith.bson.BSONMinKey
import codes.bytes.hammersmith.bson.BSONMaxKey
import akka.util.{ByteString, ByteIterator}
import codes.bytes.hammersmith.collection.Implicits.SerializableBSONDocument
import org.bson.{BasicBSONCallback, BasicBSONDecoder}
import codes.bytes.hammersmith.util._

@RunWith(classOf[JUnitRunner])
class BSONComposerSpec extends Specification with ThrownExpectations with BSONTestData with StrictLogging {

  def is =
    sequential ^
    "This specification is to test the functionality of the new BSON Composer" ^
    p ^
    "Composing BSON should" ^
    "Function without blowing up" ! testBasicCompose ^
    "Be parseable by the Java Parser" ! testJavaParse ^
    "Interoperate with the Parser, i.e. be parseable" ! testBasicParse ^
    endp
    // TODO - Test Multi-level docs as much as possibl


  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }

  def testBasicParse = {
    parsedBSON must beAnInstanceOf[Document] and not beNull
  }

  def testJavaParse = {
    val decoder = new BasicBSONDecoder
    val callback = new BasicBSONCallback
    decoder.decode(scalaBSON.toArray, callback) must not beNull
  }



}
