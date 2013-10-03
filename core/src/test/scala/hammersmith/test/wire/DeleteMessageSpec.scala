
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
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}
import com.mongodb._

@RunWith(classOf[JUnitRunner])
class DeleteMessageSpec extends Specification with ThrownExpectations with Logging {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `DeleteMessage`" ^
    p ^
    "Working with Hammersmith DeleteMessage implementations should" ^
    "Allow instantiation of a DeleteMessage" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    "Be parsed back from a composed BSON bytestream to a Document" ! testBasicParse ^
    "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
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

  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val legacy = com.mongodb.legacyDelete
    println("Legacy Message Size: " + legacy.toArray.length)
    println("Legacy Message Hex: " + hammersmith.test.hexValue(legacy.toArray))
    println("Scala Message Size: " + scalaBSON.toArray.length)
    println("Scala Message Hex: " + hammersmith.test.hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testDeleteMsg.serialize

  lazy val testDeleteMsg =
    DeleteMessage("test.deletion", Document("_id" -> "1234"), onlyRemoveOne = false)

  lazy val parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = ImmutableBSONDocumentParser(scalaBSON.iterator)
}
