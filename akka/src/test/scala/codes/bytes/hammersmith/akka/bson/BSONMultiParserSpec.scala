package codes.bytes.hammersmith.akka.bson

import _root_.akka.util.ByteString
import akka.util.ByteString
import codes.bytes.hammersmith.collection.immutable.Document
import codes.bytes.hammersmith.util._
import com.typesafe.scalalogging.StrictLogging
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ArrayBuffer

class BSONMultiParserSpec extends Specification with StrictLogging {
  sequential

  def is =
    sequential ^
      "Able to properly parse multiple documents" ! testMultiParse ^
      "Able to properly parse multiple documents from a stream" ! testMultiParseStream ^
      endp

  def multiTestDocs = {
    // todo - this is perfect for property based testing
    val rand = new scala.util.Random()

    implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

    val frameBuilder = ByteString.newBuilder

    val encoder = new org.bson.BasicBSONEncoder

    val b1 = com.mongodb.BasicDBObjectBuilder.start()
    b1.add("document", 1)
    b1.add("foo", "bar")
    b1.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b1.add("y", rand.nextLong())
    val doc1 = b1.get()
    val enc1 = encoder.encode(doc1)
    logger.debug(s"Doc 1 Length ${enc1.length} Bytes: ${hexValue(enc1)}")
    frameBuilder.putBytes(enc1)

    val b2 = com.mongodb.BasicDBObjectBuilder.start()
    b2.add("document", 2)
    b2.add("foo", "bar")
    b2.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b2.add("y", rand.nextLong())
    val doc2 = b2.get()
    val enc2 = encoder.encode(doc2)
    frameBuilder.putBytes(enc2)

    val b3 = com.mongodb.BasicDBObjectBuilder.start()
    b3.add("document", 3)
    b3.add("foo", "bar")
    b3.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b3.add("y", rand.nextLong())
    val doc3 = b3.get()
    val enc3 = encoder.encode(doc3)
    frameBuilder.putBytes(enc3)

    val b4 = com.mongodb.BasicDBObjectBuilder.start()
    b4.add("document", 4)
    b4.add("foo", "bar")
    b4.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b4.add("y", rand.nextLong())
    val doc4 = b4.get()
    val enc4 = encoder.encode(doc4)
    frameBuilder.putBytes(enc4)

    val b5 = com.mongodb.BasicDBObjectBuilder.start()
    b5.add("document", 5)
    b5.add("foo", "bar")
    b5.add("x", rand.alphanumeric.take(rand.nextInt(255)).mkString)
    b5.add("y", rand.nextLong())
    val doc5 = b5.get()
    val enc5 = encoder.encode(doc5)
    frameBuilder.putBytes(enc5)

    frameBuilder.result()
  }


  def testMultiParse = {
    val frame = multiTestDocs
    logger.debug(s"Frame for multitestDocs $frame")
    val decoded = ArrayBuffer.empty[Document]
    val iter = frame.iterator
    var count = 1
    try { {
      while (iter.hasNext) {
        logger.debug(s"*************** START: ROUND $count [frame size: ${frame.size}] *************")
        val dec = ImmutableBSONDocumentParser(iter)
        decoded += dec
        logger.debug(s"*************** END: ROUND $count [frame size: ${frame.size}] *************")
        count += 1
      }
    }
    } catch {
      case t: Throwable =>
        logger.error(s"Error: blewup in multiparse test; decoded ${decoded.size} items, from iter $iter")
    }

    decoded must haveSize(5)
  }

  def testMultiParseStream = {
    val frame = multiTestDocs
    // validated to be deferred evaluation stream.
    val decoded = ImmutableBSONDocumentParser.asStream(5, frame.iterator)
    decoded must haveSize(5)
  }
}

// vim: set ts=2 sw=2 sts=2 et: