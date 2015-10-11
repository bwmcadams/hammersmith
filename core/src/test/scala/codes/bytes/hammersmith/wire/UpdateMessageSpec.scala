
package codes.bytes.hammersmith.wire

import com.mongodb.legacyUpdate
import com.typesafe.scalalogging.StrictLogging
import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner

import org.specs2.matcher.ThrownExpectations
import codes.bytes.hammersmith.collection.immutable._
import codes.bytes.hammersmith.collection.Implicits._
import akka.util.ByteString
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}
import com.mongodb.{BasicDBObject, BasicDBObjectBuilder}
import codes.bytes.hammersmith.WriteConcern
import codes.bytes.hammersmith.util.hexValue

@RunWith(classOf[JUnitRunner])
class UpdateMessageSpec extends Specification with ThrownExpectations  with StrictLogging {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `UpdateMessage`" ^
    p ^
    "Working with Hammersmith UpdateMessage implementations should" ^
    "Allow instantiation of a UpdateMessage" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
    endp


  def testBasicInstantiation = {
    testUpdateMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }


  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val qB = BasicDBObjectBuilder.start()
    qB.add("_id", "1234")
    val q = qB.get()
    val uB = BasicDBObjectBuilder.start()
    uB.add("x", new BasicDBObject("$inc", 1))
    val u = uB.get()
    val legacy = com.mongodb.legacyUpdate("test.update", q, u, false, false)
    logger.debug("Legacy Message Size: " + legacy.toArray.length)
    logger.debug("Legacy Message Hex: " + hexValue(legacy.toArray))
    logger.debug("Scala Message Size: " + scalaBSON.toArray.length)
    logger.debug("Scala Message Hex: " + hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testUpdateMsg.serialize

  lazy val testUpdateMsg: UpdateMessage =
    UpdateMessage("test.update", Document("_id" -> "1234"), Document("x" -> Document("$inc" -> 1)))(WriteConcern.Unsafe)


}
