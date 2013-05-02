/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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

package hammersmith
package test

import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import hammersmith.collection.immutable.{OrderedDocument, DBList, Document}
import hammersmith.bson._
import java.util.regex.Pattern
import hammersmith.bson.BSONTimestamp
import hammersmith.bson.BSONMinKey
import hammersmith.bson.BSONMaxKey
import akka.util.{ByteString, ByteIterator}
import hammersmith.collection.Implicits.SerializableBSONDocument
import org.bson.{BasicBSONCallback, BasicBSONDecoder}

@RunWith(classOf[JUnitRunner])
class BSONComposerTest extends Specification with ThrownExpectations with Logging {

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


  // -- Setup Definitions
  lazy val parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = ImmutableBSONDocumentParser(scalaBSON.iterator)

  lazy val oid = ObjectID()

  lazy val testOID = ObjectID()

  lazy val testRefID = ObjectID()


  lazy val testDoc = Document("foo" -> "bar", "x" -> 5.23)

  lazy val testDBList = DBList("foo", "bar", "baz", "x", "y", "z")

  lazy val testArray = Array("foo", "bar", "baz", "x", "y", "z")

  lazy val testVector = Vector("foo", "bar", "baz", "x", "y", "z")

  lazy val testTsp = BSONTimestamp(3600, 42)

  lazy val testJDKDate = new java.util.Date

  lazy val testJavaRE = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE)

  lazy val testScalaRE = "(?i)^test.*regex.*xyz$".r

  lazy val testSymbol = 'Foobar

  lazy val testCode = BSONCode("108 * 5;")

  lazy val testCodeWScope = BSONCodeWScope("return x * 500;", testDoc)

  lazy val testBin = BSONBinary("foobarbaz".getBytes())

  lazy val testUUID = java.util.UUID.randomUUID()

  lazy val testStr = "foobarbaz"

  def scalaBSON = {
    val b = OrderedDocument.newBuilder
    b += "_id" -> oid
    b += "max" -> BSONMaxKey
    b += "min" -> BSONMinKey
    b += "booleanTrue" -> true
    b += "booleanFalse" -> false
    b += "int1" -> 1
    b += "int1500" -> 1500
    b += "int3753" -> 3753
    b += "tsp" -> testTsp
    b += "jdkDate" -> testJDKDate
    b += "long5" -> 5L
    b += "long3254525L" -> 3254525L
    b += "float324_582" -> 324.582f
    b += "double245_6289" -> 245.6289
    b += "oid" -> testOID
    b += "code" -> testCode
    b += "codeScoped" -> testCodeWScope
    b += "str" -> testStr
    //b += "ref" -> DBRef
    b += "embeddedDocument" -> testDoc
    b += "embeddedDBList" -> testDBList
    b += "embeddedArray" -> testArray
    b += "embeddedVector" -> testVector
    b += "binary" -> testBin
    b += "uuid" -> testUUID
    b += "javaRegex" -> testJavaRE
    b += "scalaRegex" -> testScalaRE
    b += "symbol" -> testSymbol
    val doc = b.result


    val bson = SerializableBSONDocument.compose(doc)
    //log.trace("BSON: " + Hex.valueOf(bson.toArray))
    bson
  }


  object Hex {
    def valueOf(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString
  }
}
