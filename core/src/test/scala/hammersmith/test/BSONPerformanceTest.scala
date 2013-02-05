package hammersmith.test

import hammersmith.bson.util.Logging
import akka.util.ByteString
import hammersmith.collection.immutable.Document
import hammersmith.bson.DefaultBSONParser
import org.bson.{BasicBSONDecoder, NewBSONDecoder}
import java.util.regex.Pattern

object BSONPerformanceTest extends App with Logging {

  scalaParserPerfTest
  newJavaParserPerfTest
  oldJavaParserPerfTest

  def perfRunCount = 50000

  def scalaParserPerfTest = {
    val start = System.currentTimeMillis()
    for (i <- 0 until perfRunCount)
      parseBSONWithScala
    val end = System.currentTimeMillis()
    val time = end - start
    log.info(s"Time to parse BSON with Scala $perfRunCount times: '$time' milliseconds.")
  }

  def newJavaParserPerfTest = {
    val start = System.currentTimeMillis()
    for (i <- 0 until perfRunCount)
      parseBSONWithNewJava
    val end = System.currentTimeMillis()
    val time = end - start
    log.info(s"Time to parse BSON with 'New' Java $perfRunCount times: '$time' milliseconds.")
  }

  def oldJavaParserPerfTest = {
    val start = System.currentTimeMillis()
    for (i <- 0 until perfRunCount)
      parseBSONWithOldJava
    val end = System.currentTimeMillis()
    val time = end - start
    log.info(s"Time to parse BSON with 'Old' Java $perfRunCount times: '$time' milliseconds.")
  }

  // -- Setup definitions

  lazy val oid = new org.bson.types.ObjectId

  lazy val testOid = new org.bson.types.ObjectId

  lazy val testRefId = new org.bson.types.ObjectId

  lazy val testDoc = {
    val t = new com.mongodb.BasicDBObject
    t.put("foo", "bar")
    t.put("x", 5.23)
    t
  }

  lazy val testList = {
    val t = new java.util.ArrayList[String]
    t.add("foo")
    t.add("bar")
    t.add("baz")
    t.add("x")
    t.add("y")
    t.add("z")
    t
  }

  lazy val testTsp = new org.bson.types.BSONTimestamp(3600, 42)

  lazy val testDate = new java.util.Date()

  lazy val testRE = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE)

  lazy val testSym = new org.bson.types.Symbol("foobar")

  lazy val testCode = new org.bson.types.Code("var x = 12345")

  lazy val testBin = new org.bson.types.Binary("foobarbaz".getBytes())

  lazy val testUUID = java.util.UUID.randomUUID()

  lazy val testCodeWScope = new org.bson.types.CodeWScope("return x * 500;", testDoc)

  lazy val testStr = "foobarbaz"

  lazy val javaBSON = {

    val b = com.mongodb.BasicDBObjectBuilder.start()
    b.append("_id", oid)
    b.append("null", null)
    b.append("max", new org.bson.types.MaxKey())
    b.append("min", new org.bson.types.MinKey())
    b.append("booleanTrue", true)
    b.append("booleanFalse", false)
    b.append("int1", 1)
    b.append("int1500", 1500)
    b.append("int3753", 3753)
    b.append("tsp", testTsp)
    b.append("date", testDate)
    b.append("long5", 5L)
    b.append("long3254525", 3254525L)
    b.append("float324_582", 324.582f)
    b.append("double245_6289", 245.6289)
    b.append("oid", testOid)
    // Code wonky
    b.append("code", testCode)
    b.append("code_scoped", testCodeWScope)
    b.append("str", testStr)
    //b.append("ref", new com.mongodb.DBRef(_db, "testRef", test_ref_id))
    b.append("object", testDoc)
    b.append("array", testList)
    b.append("binary", testBin)
    b.append("uuid", testUUID)
    b.append("regex", testRE)
    // Symbol wonky in java driver
    //b.append("symbol", testSym)

    val doc = b.get()

    val encoder = new org.bson.BasicBSONEncoder

    encoder.encode(doc)
  }


  lazy val parsedBSON: Document = parseBSONWithScala

  def parseBSONWithScala: Document = DefaultBSONParser.unapply(ByteString(javaBSON).iterator)

  // Test with the "New" Decoder which is probably the performance guideline going forward
  def parseBSONWithNewJava = {
    newJavaParser.readObject(javaBSON)
  }

  def parseBSONWithOldJava = {
    oldJavaParser.readObject(javaBSON)
  }


  private lazy val newJavaParser = new NewBSONDecoder()

  private lazy val oldJavaParser = new BasicBSONDecoder()
}
