/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
import runner._

import scala.collection.JavaConverters._

import java.util.regex._

import hammersmith.collection._
import hammersmith.collection.Implicits._
import bson._
import akka.util.ByteString
import bson.BSONMinKey
import bson.BSONMaxKey
import hammersmith.collection.immutable.Document
import hammersmith.bson.util.Logging

@RunWith(classOf[JUnitRunner])
class BSONTest extends Specification with Logging {
  def is =

    "This is a specification to test the functionality of BSON" ^
      p ^
      "Parsing of BSON should" ^
      "Provide clear, valid, and sane interop w/ old Java driver" ^
      "Parsing returns a valid document, checking fields" ! testBasicParse ^
      "_id" ! hasOID ^
      "null" ! hasNull ^
      "maxKey" ! hasMax ^
      "minKey" ! hasMin ^
      "booleanTrue" ! hasBoolTrue ^
      "booleanFalse" ! hasBoolFalse ^
      "int1" ! hasInt1 ^
      "int1500" ! hasInt1500 ^
      "int3753" ! hasInt3753 ^
      "tsp" ! hasTsp ^
      "date" ! hasDate ^
      "long5" ! hasLong5 ^
      "long3254525" ! hasLong3254525 ^
      "float324_582" ! hasFloat324_582 ^
      "double245_6289" ! hasDouble245_6289 ^
      "another OID" ! hasOtherOID ^
      /** "symbol" ! hasSymbol ^ SYMBOL DOESNT ENCODE PROEPRLY FROM JAVA */
      "code" ! hasCode ^
      "scoped code, code" ! hasScopedCode_Code ^
      "scoped code, scope" ! hasScopedCode_Scope ^
      "str" ! hasStr ^
      "object" ! hasSubObj ^
      "array" ! hasArray ^
      "binary" ! hasBytes ^
      "uuid" ! hasUUID ^
      end

  def testBasicParse = {
    System.err.println("Doc: " + parsedBSON)
    System.err.println("*** List : " + parsedBSON.get("array"))
    parsedBSON must haveClass[Document] and not beNull
  }

  def hasOID = parsedBSON.get("_id") must beSome.which(_.toString == oid.toString)

  def hasNull = parsedBSON.get("null") must beSome(null) // BSON Null is fucking stupid.

  def hasMax = parsedBSON.get("max") must beSome(BSONMaxKey())

  def hasMin = parsedBSON.get("min") must beSome(BSONMinKey())

  def hasBoolTrue = parsedBSON.get("booleanTrue") must beSome(true)

  def hasBoolFalse = parsedBSON.get("booleanFalse") must beSome(false)

  def hasInt1 = parsedBSON.get("int1") must beSome(1)

  def hasInt1500 = parsedBSON.get("int1500") must beSome(1500)

  def hasInt3753 = parsedBSON.get("int3753") must beSome(3753)

  def hasTsp = parsedBSON.getAs[BSONTimestamp]("tsp") must beSome(BSONTimestamp(testTsp.getTime, testTsp.getInc))

  def hasDate = parsedBSON.getAs[java.util.Date]("date") must beSome(testDate)

  def hasLong5 = parsedBSON.get("long5") must beSome(5L)

  def hasLong3254525 = parsedBSON.get("long3254525") must beSome(3254525L)

  def hasFloat324_582 = parsedBSON.get("float324_582") must beSome(324.582f)

  def hasDouble245_6289 = parsedBSON.get("double245_6289") must beSome(245.6289)

  def hasOtherOID = parsedBSON.get("oid") must beSome.which(_.toString == testOid.toString)

  def hasSymbol = parsedBSON.getAs[Symbol]("symbol") must beSome(testSym.getSymbol())

  def hasCode = parsedBSON.getAs[BSONCode]("code") must beSome.which(_.code == testCode.getCode())

  def hasScopedCode_Code = parsedBSON.getAs[BSONCodeWScope]("code_scoped") must beSome.which(_.code == testCodeWScope.getCode())
  
  def hasScopedCode_Scope = parsedBSON.getAs[BSONCodeWScope]("code_scoped").get.scope must havePairs("foo" -> "bar", "x"-> 5.23)
  
  def hasStr = parsedBSON.getAs[String]("str") must beSome(testStr)
  
  def hasSubObj = parsedBSON.getAs[Document]("object") must beSome.which(_ must havePairs("foo" -> "bar", "x" -> 5.23))
      
  def hasArray = parsedBSON.getAs[BSONList]("array") must beSome.which(_ must contain("foo", "bar", "baz", "x", "y", "z"))
  
  def hasBytes = parsedBSON.getAs[BSONBinary]("binary") must beSome.which(_.bytes must beEqualTo(testBin.getData()))

  def hasUUID =  parsedBSON.getAs[java.util.UUID]("uuid") must beSome.which { _ must beEqualTo(testUUID) }

  // -- Setup definitions

  lazy val oid = new org.bson.types.ObjectId

  lazy val testOid = new org.bson.types.ObjectId

  lazy val testRefId = new org.bson.types.ObjectId

  lazy val testDoc = {
    val t = new com.mongodb.BasicDBObject
    t.put("foo", "bar")
    t.put("x", 5.23)
    t
  }

  lazy val testList = {
    val t = new java.util.ArrayList[String]
    t.add("foo")
    t.add("bar")
    t.add("baz")
    t.add("x")
    t.add("y")
    t.add("z")
    t
  }

  lazy val testTsp = new org.bson.types.BSONTimestamp(3600, 42)

  lazy val testDate = new java.util.Date()

  lazy val testRE = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE)

  lazy val testSym = new org.bson.types.Symbol("foobar")

  lazy val testCode = new org.bson.types.Code("var x = 12345")

  lazy val testBin = new org.bson.types.Binary("foobarbaz".getBytes())

  lazy val testUUID = java.util.UUID.randomUUID()
  
  lazy val testCodeWScope = new org.bson.types.CodeWScope("return x * 500;", testDoc)

  lazy val testStr = "foobarbaz"
   
    
  lazy val javaBSON = {

    val b = com.mongodb.BasicDBObjectBuilder.start()
    b.append("_id", oid)
    b.append("null", null)
    b.append("max", new org.bson.types.MaxKey())
    b.append("min", new org.bson.types.MinKey())
    b.append("booleanTrue", true)
    b.append("booleanFalse", false)
    b.append("int1", 1)
    b.append("int1500", 1500)
    b.append("int3753", 3753)
    b.append("tsp", testTsp)
    b.append("date", testDate)
    b.append("long5", 5L)
    b.append("long3254525", 3254525L)
    b.append("float324_582", 324.582f)
    b.append("double245_6289", 245.6289)
    b.append("oid", testOid)
    // Code wonky
    b.append("code", testCode)
    b.append("code_scoped", testCodeWScope)
    b.append("str", testStr)
    //b.append("ref", new com.mongodb.DBRef(_db, "testRef", test_ref_id))
    b.append("object", testDoc)
    b.append("array", testList)
    b.append("binary", testBin)
    b.append("uuid", testUUID)
    b.append("regex", testRE)
    // Symbol wonky in java driver
    //b.append("symbol", testSym)

    val doc = b.get()

    val encoder = new org.bson.BasicBSONEncoder

    java.nio.ByteBuffer.wrap(encoder.encode(doc))
  }

  lazy val parsedBSON: Document = {
    val p = DefaultBSONParser.unapply(ByteString(javaBSON).iterator)
    p
  }

}