/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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
package codes.bytes.hammersmith

import com.typesafe.scalalogging.StrictLogging
import org.specs2._
import org.junit.runner._
import runner._

import scala.collection.JavaConverters._

import java.util.regex._

import codes.bytes.hammersmith.collection._
import codes.bytes.hammersmith.collection.Implicits._
import bson._
import akka.util.ByteString
import bson.BSONMinKey
import bson.BSONMaxKey
import codes.bytes.hammersmith.collection.immutable.Document
import org.bson.{BasicBSONDecoder, NewBSONDecoder}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class BSONParserSpec extends Specification with StrictLogging {
  sequential

  def is =

    "This is a specification to test the functionality of the new BSON Parser" ^
      p ^
      "Parsing of BSON should" ^
      "Provide clear, valid, and sane interop w/ old 10gen Java driver" ^
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
      /** "symbol" ! hasSymbol ^ SYMBOL DOESNT ENCODE PROPERLY FROM JAVA */
      "code" ! hasCode ^
      "scoped code, code" ! hasScopedCode_Code ^
      "scoped code, scope" ! hasScopedCode_Scope ^
      "str" ! hasStr ^
      "object" ! hasSubObj ^
      "array" ! hasArray ^
      "binary" ! hasBytes ^
      "uuid" ! hasUUID ^
      endp ^
      "Parsing of a list of documents" ! testMultiParse ^
      "Parsing a list of docs as a stream"  ! testMultiParseStream

      /*
      "Perform somewhat sanely" ^
        "Scala Parser Performs" ! scalaParserPerfTest ^
        "'New' Java Parser Performs" ! newJavaParserPerfTest ^
        "'Old' Java Parser Performs" ! oldJavaParserPerfTest ^*/


  def testBasicParse = {
    parsedBSON must haveClass[Document] and not beNull
  }

  def hasOID = parsedBSON.get("_id") must beSome.which(_.toString == oid.toString)

  def hasNull = parsedBSON.get("null") must beSome(BSONNull) // BSON Null is fucking stupid.

  def hasMax = parsedBSON.get("max") must beSome(BSONMaxKey)

  def hasMin = parsedBSON.get("min") must beSome(BSONMinKey)

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

    encoder.encode(doc)
  }


  lazy val parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = ImmutableBSONDocumentParser(ByteString(javaBSON).iterator)

  // Test with the "New" Decoder which is probably the performance guideline going forward
  def parseBSONWithNewJava = {
    newJavaParser.readObject(javaBSON)
  }

  def parseBSONWithOldJava = {
    oldJavaParser.readObject(javaBSON)
  }


  private val newJavaParser = new NewBSONDecoder()

  private val oldJavaParser = new BasicBSONDecoder()

  def multiTestDocs =  {
    val rand = new scala.util.Random()

    implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

    val frameBuilder = ByteString.newBuilder

    val encoder = new org.bson.BasicBSONEncoder

    val b1 = com.mongodb.BasicDBObjectBuilder.start()
    b1.add("document", 1)
    b1.add("foo", "bar")
    b1.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b1.add("y", rand.nextLong())
    val doc1 = b1.get()
    val enc1 = encoder.encode(doc1)
    frameBuilder.putBytes(enc1)

    val b2 = com.mongodb.BasicDBObjectBuilder.start()
    b2.add("document", 2)
    b2.add("foo", "bar")
    b2.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b2.add("y", rand.nextLong())
    val doc2 = b2.get()
    val enc2 = encoder.encode(doc2)
    frameBuilder.putBytes(enc2)

    val b3 = com.mongodb.BasicDBObjectBuilder.start()
    b3.add("document", 3)
    b3.add("foo", "bar")
    b3.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b3.add("y", rand.nextLong())
    val doc3 = b3.get()
    val enc3 = encoder.encode(doc3)
    frameBuilder.putBytes(enc3)

    val b4 = com.mongodb.BasicDBObjectBuilder.start()
    b4.add("document", 4)
    b4.add("foo", "bar")
    b4.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b4.add("y", rand.nextLong())
    val doc4 = b4.get()
    val enc4 = encoder.encode(doc4)
    frameBuilder.putBytes(enc4)

    val b5 = com.mongodb.BasicDBObjectBuilder.start()
    b5.add("document", 5)
    b5.add("foo", "bar")
    b5.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b5.add("y", rand.nextLong())
    val doc5 = b5.get()
    val enc5 = encoder.encode(doc5)
    frameBuilder.putBytes(enc5)

    frameBuilder.result()
  }

  def testMultiParse = {
    val frame = multiTestDocs
    logger.debug(s"Frame for multitestDocs $frame")
    val decoded = ArrayBuffer.empty[Document]
    val iter = frame.iterator
    logger.debug(s"Iter for multiTestDocs: $iter")
    try {
      while (iter.hasNext) {
        val dec = ImmutableBSONDocumentParser(iter)
        decoded += dec
      }
    } catch {
      case t =>
        logger.error(s"Error: blewup in multiparse test; decoded ${decoded.size} items, from iter $iter", t)
    }

    decoded must haveSize(5)
  }

  def testMultiParseStream = {
    val frame = multiTestDocs
    // validated to be deferred evaluation stream.
    val decoded = ImmutableBSONDocumentParser.asStream(5, frame.iterator)
    decoded must haveSize(5)
  }


}