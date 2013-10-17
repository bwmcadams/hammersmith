
package hammersmith.test.wire

import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import hammersmith.collection.immutable._
import hammersmith.collection.Implicits._
import akka.util.ByteString
import org.bson.{BasicBSONEncoder, BasicBSONCallback, BasicBSONDecoder}
import hammersmith.wire.InsertMessage
import hammersmith.WriteConcern

@RunWith(classOf[JUnitRunner])
class InsertMessageSpec extends Specification with ThrownExpectations with Logging {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16

  def is =
    sequential ^
    "This specification is to test the functionality of the Wire Protocol `InsertMessage`" ^
    p ^
    "Working with Hammersmith Single InsertMessage implementations should" ^
    "Allow instantiation of a Single InsertMessage" ! testBasicInstantiation ^
    "Be composed into a BSON bytestream" ! testBasicCompose ^
    endp ^
    "Working with Hammersmith Bulk InsertMessage implementations should"  ^
      "Allow instantiation of a Bulk InsertMessage" ! testBasicBulkInstantiation ^
      "Be composed into a BSON bytestream" ! testBasicBulkCompose ^
    endp


  def testBasicInstantiation = {
    testInsertMsg must not beNull
  }

  def testBasicCompose = {
    scalaBSON must beAnInstanceOf[ByteString] and not beNull
  }

  def testBasicBulkInstantiation = {
    testBulkInsertMsg must not beNull
  }

  def testBasicBulkCompose = {
    scalaBulkBSON must beAnInstanceOf[ByteString] and not beNull
  }


  lazy val scalaBSON = testInsertMsg.serialize
  lazy val scalaBulkBSON = testBulkInsertMsg.serialize

  lazy val testInsertMsg: InsertMessage =
    InsertMessage("test.insert", false, Document("foo" -> "bar"))(WriteConcern.Unsafe)

  lazy val testBulkInsertMsg: InsertMessage =
    InsertMessage("test.insert", false, Document("foo" -> "bar"), Document("spam" -> "eggs"))(WriteConcern.Unsafe)


}
