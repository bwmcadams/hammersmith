
package codes.bytes.hammersmith.wire

import com.mongodb.legacyQuery
import codes.bytes.hammersmith.hexValue
import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import codes.bytes.hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import codes.bytes.hammersmith.collection.immutable._
import codes.bytes.hammersmith.collection._
import akka.util.ByteString
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}
import com.mongodb.BasicDBObjectBuilder
import codes.bytes.hammersmith.wire.QueryMessage

@RunWith(classOf[JUnitRunner])
class QueryMessageSpec extends Specification with ThrownExpectations with Logging {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `QueryMessage`" ^
    p ^
    "Working with Hammersmith QueryMessage implementations should" ^
    "Allow instantiation of a QueryMessage" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    "Be comparable to a message created by the MongoDB Java Driver's BSON routines" ! testEncoding ^
    endp


  def testBasicInstantiation = {
    testQueryMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }


  def testEncoding = {
    val decoder = new BasicBSONDecoder
    val encoder = new BasicBSONEncoder
    val callback = new BasicBSONCallback
    val q = BasicDBObjectBuilder.start()
    q.add("_id", "1234")
    val legacy = com.mongodb.legacyQuery("test.query", 10, 10, q.get(), None, false,
                                         false, false, false, false, false)
    println("Legacy Message Size: " + legacy.toArray.length)
    println("Legacy Message Hex: " + hexValue(legacy.toArray))
    println("Scala Message Size: " + scalaBSON.toArray.length)
    println("Scala Message Hex: " + hexValue(scalaBSON.toArray))
    scalaBSON.toArray must beEqualTo(legacy)

  }


  lazy val scalaBSON = testQueryMsg.serialize

  lazy val testQueryMsg: QueryMessage =
    QueryMessage("test.query", 10, 10, Document("_id" -> "1234"))

}
