
package codes.bytes.hammersmith.test.wire

import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import codes.bytes.hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import codes.bytes.hammersmith.collection.immutable._
import codes.bytes.hammersmith.bson._
import akka.util.ByteString
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}
import codes.bytes.hammersmith.wire.GetMoreMessage

@RunWith(classOf[JUnitRunner])
class GetMoreMessageSpec extends Specification with ThrownExpectations with Logging {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `GetMoreMessage`" ^
    p ^
    "Working with Hammersmith GetMoreMessage implementations should" ^
    "Allow instantiation of a GetMore" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
    endp


  def testBasicInstantiation = {
    testGetMoreMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }


  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val legacy = com.mongodb.legacyGetMore(150, 102)
    println("Legacy Message Size: " + legacy.toArray.length)
    println("Legacy Message Hex: " + hammersmith.test.hexValue(legacy.toArray))
    println("Scala Message Size: " + scalaBSON.toArray.length)
    println("Scala Message Hex: " + hammersmith.test.hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testGetMoreMsg.serialize

  lazy val testGetMoreMsg =
    GetMoreMessage("test.getMore", 150, 102)

}
