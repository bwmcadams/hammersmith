
package codes.bytes.hammersmith.akka.wire

import akka.util.ByteString
import codes.bytes.hammersmith._
import codes.bytes.hammersmith.akka.wire.DeleteMessage
import codes.bytes.hammersmith.collection.Implicits._
import codes.bytes.hammersmith.collection.immutable._
import codes.bytes.hammersmith.util.hexValue
import codes.bytes.hammersmith.wire.WriteConcern
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner._
import org.specs2._
import org.specs2.matcher.ThrownExpectations
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeleteMessageSpec extends Specification with ThrownExpectations with StrictLogging {
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
    logger.debug("Legacy Message Size: " + legacy.toArray.length)
    logger.debug("Legacy Message Hex: " + hexValue(legacy.toArray))
    logger.debug("Scala Message Size: " + scalaBSON.toArray.length)
    logger.debug("Scala Message Hex: " + hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testDeleteMsg.serialize

  lazy val testDeleteMsg: DeleteMessage =
    DeleteMessage("test.deletion", Document("_id" -> "1234"), onlyRemoveOne = false)(WriteConcern.Unsafe)

}
