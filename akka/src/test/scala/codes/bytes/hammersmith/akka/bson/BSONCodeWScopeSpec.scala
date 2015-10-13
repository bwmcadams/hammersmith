package codes.bytes.hammersmith.akka.bson

import codes.bytes.hammersmith.bson.types.BSONCodeWScope
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BSONCodeWScopeSpec extends Specification with BSONTestData with StrictLogging {

  def is =
    sequential ^
      "This specification tests the intermittently broken and WTF code of Scoped JS Code types in MongoDB" ^
      p ^
      "scoped code, code" ! hasScopedCode_Code ^
      "scoped code, scope" ! hasScopedCode_Scope ^
      endp

  def hasScopedCode_Code = {
    parsedBSON.getAs[BSONCodeWScope]("codeScoped") must beSome.which(_.code == testCodeWScope.code)
  }

  def hasScopedCode_Scope = {
    parsedBSON.getAs[BSONCodeWScope]("codeScoped").get.scope must havePairs("foo" -> "bar", "x" -> 5.23)
  }
}

// vim: set ts=2 sw=2 sts=2 et: