package codes.bytes.hammersmith.akka.bson

import codes.bytes.hammersmith.bson.types.{BSONJSCode, BSONScopedJSCode}
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner

class BSONCodeWScopeSpec extends Specification with BSONTestData with StrictLogging {

  def is =
    skipAllIf(1 == 1) ^ // temporarily disable
      sequential ^
      "This specification tests the intermittently broken and WTF code of Scoped JS Code types in MongoDB" ^
      p ^
      "scoped code, code" ! hasScopedCode_Code ^
      "scoped code, scope" ! hasScopedCode_Scope ^
      endp

  def hasScopedCode_Code = {
    parsedBSON.getAs[BSONJSCode]("codeScoped") must beSome
  }

  def hasScopedCode_Scope = {
    parsedBSON.getAs[BSONScopedJSCode]("codeScoped") must beSome
  }
}

// vim: set ts=2 sw=2 sts=2 et:
