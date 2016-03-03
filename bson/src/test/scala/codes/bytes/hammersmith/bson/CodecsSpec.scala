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

import codes.bytes.hammersmith.bson.codecs.BSONCodec
import codes.bytes.hammersmith.bson.types._
import com.mongodb.{BasicDBObject, DBObject, MongoClient}
import com.mongodb.connection.ByteBufferBsonOutput
import org.bson.{BsonDocumentWriter, BasicBSONEncoder, Document}
import org.scalatest.{OptionValues, MustMatchers, WordSpec}
import scodec.Codec
import scodec.bits.BitVector
import scala.collection.JavaConversions._
import org.scalatest.OptionValues._


class CodecsSpec extends WordSpec with MustMatchers with OptionValues {
  import codes.bytes.hammersmith.bson.util._

  "The AST Based BSON Codec" should {
    "Decode a known BSON byte array" in {
      1 mustEqual 1
    }
    "Decode a BSON Document encoded by Mongo's Java Driver" in {
      import BSONCodec.bsonFieldCodec
      val inBytes = testDBObject()
      val inBits = BitVector(inBytes)
      val outDoc = BSONCodec.decode(inBits)

      println(outDoc)
      outDoc must be ('defined)

      val map = Map(outDoc.value.entries: _*)

      map must ( contain.key("foo") and contain.key("x") and contain.key("pi") )

      map.get("foo").value mustBe BSONString("bar")
      map.get("x").value mustBe BSONInteger(5)
      map.get("pi").value mustBe BSONDouble(3.14)
    }
  }
  // todo make sure we use DBObject *AND* org.bson.Document in perf tests
  def testDBObject(): Array[Byte] = {
    val doc = new BasicDBObject().
      append("pi", 3.14).
      append("x", 5).
      append("foo", "bar")
    val enc = new BasicBSONEncoder()
    val bytes = enc.encode(doc)
    println("DBObject Bytes: " + hexValue(bytes))
    bytes
  }
  /*
  val w = new ByteBufferBsonOutput();
  val doc = new Document().
    append("foo", "bar").
    append("x", 5).
    append("pi", 3.14)
  val enc = MongoClient.getDefaultCodecRegistry().get(classOf[Document])
  enc.encode(BSONBi)
  */

}


// vim: set ts=2 sw=2 sts=2 et:
