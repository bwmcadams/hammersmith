package codes.bytes.hammersmith

import java.util.regex.Pattern

import codes.bytes.hammersmith.bson._
import codes.bytes.hammersmith.collection.Implicits.SerializableBSONDocument
import codes.bytes.hammersmith.collection.immutable.{OrderedDocument, DBList, Document}
import codes.bytes.hammersmith.util._
import com.typesafe.scalalogging.StrictLogging

trait BSONTestData extends StrictLogging {
  // -- Setup Definitions
  def parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = ImmutableBSONDocumentParser(scalaBSON.iterator)

  lazy val oid = ObjectID()

  lazy val testOID = ObjectID()

  lazy val testRefID = ObjectID()


  lazy val testDoc = Document("foo" -> "bar", "x" -> 5.23)

  lazy val testDBList = DBList("foo", "bar", "baz", "x", "y", "z")

  lazy val testArray = Array("foo", "bar", "baz", "x", "y", "z")

  lazy val testVector = Vector("foo", "bar", "baz", "x", "y", "z")

  lazy val testTsp = BSONTimestamp(3600, 42)

  lazy val testJDKDate = new java.util.Date

  lazy val testJavaRE = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE)

  lazy val testScalaRE = "(?i)^test.*regex.*xyz$".r

  lazy val testSymbol = 'Foobar

  lazy val testCode = BSONCode("108 * 5;")

  lazy val testCodeWScope = BSONCodeWScope("return x * 500;", testDoc)

  lazy val testBin = BSONBinary("foobarbaz".getBytes())

  lazy val testUUID = java.util.UUID.randomUUID()

  lazy val testStr = "foobarbaz"

  def scalaBSON = {
    val b = OrderedDocument.newBuilder

    b += "_id" -> oid

    b += "max" -> BSONMaxKey
    b += "min" -> BSONMinKey
    b += "booleanTrue" -> true
    b += "booleanFalse" -> false
    b += "int1" -> 1
    b += "int1500" -> 1500
    b += "int3753" -> 3753
    b += "tsp" -> testTsp
    b += "jdkDate" -> testJDKDate
    b += "long5" -> 5L
    b += "long3254525L" -> 3254525L
    b += "float324_582" -> 324.582f
    b += "double245_6289" -> 245.6289
    b += "oid" -> testOID
    b += "code" -> testCode
    // Problem starts here, with testCodeWScope... related to embedded doc?

    b += "codeScoped" -> testCodeWScope
    b += "str" -> testStr
    //b += "ref" -> DBRef
    b += "embeddedDocument" -> testDoc
    b += "embeddedDBList" -> testDBList
    b += "embeddedArray" -> testArray
    b += "embeddedVector" -> testVector
    b += "binary" -> testBin
    b += "uuid" -> testUUID
    b += "javaRegex" -> testJavaRE
    b += "scalaRegex" -> testScalaRE
    b += "symbol" -> testSymbol
    val doc = b.result()


    val bson = SerializableBSONDocument.compose(doc)
    logger.trace(s"BSON: ${hexValue(bson.toArray)}")
    bson
  }

}

// vim: set ts=2 sw=2 sts=2 et: