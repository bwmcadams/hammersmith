
package hammersmith.wire
package test

import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import hammersmith.collection.Implicits._
import hammersmith.collection.immutable._
import hammersmith.bson._
import hammersmith._
import java.util.regex.Pattern
import hammersmith.bson.BSONTimestamp
import hammersmith.bson.BSONMinKey
import hammersmith.bson.BSONMaxKey
import akka.util.{ByteString, ByteIterator}
import hammersmith.collection.Implicits.SerializableBSONDocument
import org.bson.{BasicBSONCallback, BasicBSONDecoder}

@RunWith(classOf[JUnitRunner])
class DeleteMessageSpec extends Specification with ThrownExpectations with Logging {

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `DeleteMessage`" ^
    p ^
    "Working with Hammersmith DeleteMessage implementations should" ^
    "Allow instantiation of a DeleteMessage" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    "Be parsed back from a composed BSON bytestream to a Document" ! testBasicParse ^
    endp


  def testBasicInstantiation = {
    testDeleteMsg must not beNull
  }

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

  lazy val scalaBSON = testDeleteMsg.serialize

  lazy val testDeleteMsg =
    DeleteMessage("test.deletion", Document("_id" -> ObjectID()), onlyRemoveOne = false)

  lazy val parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = ImmutableBSONDocumentParser(scalaBSON.iterator)
}
