
package codes.bytes.hammersmith.wire

import com.mongodb.legacyDelete
import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import codes.bytes.hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import codes.bytes.hammersmith.collection.Implicits._
import codes.bytes.hammersmith.collection.immutable._
import codes.bytes.hammersmith.bson._
import codes.bytes.hammersmith._
import java.util.regex.Pattern
import codes.bytes.hammersmith.bson.BSONTimestamp
import codes.bytes.hammersmith.bson.BSONMinKey
import codes.bytes.hammersmith.bson.BSONMaxKey
import akka.util.{ByteString, ByteIterator}
import codes.bytes.hammersmith.collection.Implicits.SerializableBSONDocument
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}

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
    "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
    endp


  def testBasicInstantiation = {
    testDeleteMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }


  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val legacy = com.mongodb.legacyDelete("1234")
    println("Legacy Message Size: " + legacy.toArray.length)
    println("Legacy Message Hex: " + hexValue(legacy.toArray))
    println("Scala Message Size: " + scalaBSON.toArray.length)
    println("Scala Message Hex: " + hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testDeleteMsg.serialize

  lazy val testDeleteMsg: DeleteMessage =
    DeleteMessage("test.deletion", Document("_id" -> "1234"), onlyRemoveOne = false)(WriteConcern.Unsafe)

}
