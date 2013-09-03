
package hammersmith.wire
package test

import org.specs2._
import org.junit.runner._
import org.specs2.runner.JUnitRunner
import hammersmith.util.Logging
import org.specs2.matcher.ThrownExpectations
import hammersmith.collection.immutable.{OrderedDocument, DBList, Document}
import hammersmith.bson._
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
    endp
}
