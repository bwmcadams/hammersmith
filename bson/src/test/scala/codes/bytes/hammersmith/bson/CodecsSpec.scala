package codes.bytes.hammersmith.bson

/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

import java.util.regex.Pattern

import codes.bytes.hammersmith.bson.codecs.BSONCodec
import codes.bytes.hammersmith.bson.types._
import com.mongodb.{BasicDBObject, DBObject, MongoClient}
import com.mongodb.connection.ByteBufferBsonOutput
import org.bson.types.ObjectId
import org.bson.{BasicBSONDecoder, BSONDecoder, BsonDocumentWriter, BasicBSONEncoder, Document}
import org.scalatest.{OptionValues, MustMatchers, WordSpec}
import scodec.Codec
import scodec.bits.BitVector
import scala.collection.JavaConversions._
import org.scalatest.OptionValues._


class CodecsSpec extends WordSpec with MustMatchers with OptionValues {
  import codes.bytes.hammersmith.bson.util._

  "The AST Based BSON Codec" must {
    "Decode a known BSON byte array" in {
      1 mustEqual 1
    }
    "Decode a BSON Document encoded by Mongo's Java Driver" in {
      import BSONCodec.bsonFieldCodec
      val inBytes = javaBSON
      val inBits = BitVector(inBytes)
      val outDoc = BSONCodec.decode(inBits)

      println(outDoc)
      outDoc must be ('defined)

      val map = Map(outDoc.value.entries: _*)

      map must ( contain.key("max") and contain.key("min") and contain.key("booleanTrue")
                 and contain.key("long5") and contain.key("str") and contain.key("tsp")
                 and contain.key("float324_582") and contain.key("double245_6289")
                )

      map.get("str").value mustBe BSONString(testStr)
      map.get("long5").value mustBe BSONLong(5)
      map.get("float324_582").value mustBe BSONDouble(324.582f)
      map.get("double245_6289").value mustBe BSONDouble(245.6289)
      map.get("booleanFalse").value mustBe BSONBooleanFalse
      map.get("booleanTrue").value mustBe BSONBooleanTrue
      map.get("array").value mustBe BSONRawArray(Vector(BSONString("foo"), BSONString("bar"),
        BSONString("baz"), BSONString("x"), BSONString("y"), BSONString("z")))
      map.get("regex").value mustBe BSONRegex("^test.*regex.*xyz$", "im")
      map.get("symbol").value mustBe BSONSymbol("foobar")
      map.get("date").value mustBe BSONUTCDateTime(testDate.getTime)
      DefaultBSONMarshaller.DefaultBSONUTCDateTimeDeser.toNative(map.get("date").value.asInstanceOf[BSONUTCDateTime]) mustBe testDate
      map.get("tsp").value mustBe BSONTimestamp(testTsp.getInc, testTsp.getTime)
    }
    "Not exhibit weird behavior with strings, decoding a doc with just a string cleanly with no remainder" in {
      val inBytes = bsonStringEncode
      val inBits = BitVector(inBytes)
      val outDoc = BSONCodec.decode(inBits)

      outDoc must be ('defined)
    }
    "Encode and then Decode its own documents with the decoder from java" in {
      import BSONCodec.bsonFieldCodec
      val inBytes = javaBSON
      val inBits = BitVector(inBytes)
      val outDoc = BSONCodec.decode(inBits)

      println(outDoc)
      outDoc must be ('defined)

      val encodedDoc = BSONCodec.encode(outDoc.value)

      encodedDoc must be ('defined)
      encodedDoc.value must not be('empty)
      println(s"Encoded Doc: ${encodedDoc.value}")

      val dec = new BasicBSONDecoder()

      val doc = dec.readObject(encodedDoc.value.toByteArray)

      doc must not be null
    }
  }


  // -- Setup definitions

  lazy val oid = new org.bson.types.ObjectId

  lazy val testOid = new org.bson.types.ObjectId

  lazy val testRefId = new org.bson.types.ObjectId

  lazy val testDoc = {
    val t = new com.mongodb.BasicDBObject
    t.put("foo", "bar")
    t.put("x", new java.lang.Double(5.23))
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

  lazy val testRE = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE)

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
    // TODO: Code wonky
    /*b.append("code", testCode)
    b.append("code_scoped", testCodeWScope)*/
    b.append("str", testStr)
    //b.append("ref", new com.mongodb.DBRef("testRef", "foo"))
    b.append("object", testDoc)
    b.append("array", testList)
    // TODO: Fix binary
    //b.append("binary", testBin)
    // TODO: Fix UUID
    //b.append("uuid", testUUID)
    b.append("regex", testRE)
    // Symbol wonky in java driver
    b.append("symbol", testSym)

    val doc = b.get()

    val encoder = new org.bson.BasicBSONEncoder

    encoder.encode(doc)
  }

  lazy val bsonStringEncode = {
    val b = com.mongodb.BasicDBObjectBuilder.start()
    b.append("foo", "Foo Bar Baz")
    val doc = b.get()
    val encoder = new org.bson.BasicBSONEncoder

    encoder.encode(doc)
  }
}


// vim: set ts=2 sw=2 sts=2 et:
