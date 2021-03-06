
package codes.bytes.hammersmith.akka.wire

import akka.util.ByteString
import codes.bytes.hammersmith.util.hexValue
import com.typesafe.scalalogging.StrictLogging
import org.bson.{BasicBSONCallback, BasicBSONEncoder, BasicBSONDecoder}
import org.junit.runner._
import org.specs2._
import org.specs2.matcher.ThrownExpectations
import org.specs2.runner.JUnitRunner

class KillCursorsMessageSpec extends Specification with ThrownExpectations with StrictLogging {
  /**
    * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
    */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
      skipAllIf(1 == 1) ^ // temporarily disable
      "This specification is to test the functionality of the Wire Protocol `KillCursorsMessage`" ^
      p ^
      "Working with Hammersmith KillCursorsMessage implementations should" ^
      "Allow instantiation of a KillCursors" ! testBasicInstantiation ^
      "Be composed into a BSON bytestream" ! testBasicCompose ^
      "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
      endp


  def testBasicInstantiation = {
    testKillCursorsMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }


  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val legacy = com.mongodb.legacyKillCursors(1L to 10L)
    logger.debug("Legacy Message Size: " + legacy.toArray.length)
    logger.debug("Legacy Message Hex: " + hexValue(legacy.toArray))
    logger.debug("Scala Message Size: " + scalaBSON.toArray.length)
    logger.debug("Scala Message Hex: " + hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testKillCursorsMsg.serialize

  lazy val testKillCursorsMsg =
    KillCursorsMessage(1L to 10L)

}
