
package hammersmith.io

import akka.io._
import hammersmith.wire.{MongoClientWriteMessage, MongoServerMessage}
import akka.util.ByteString
import hammersmith.bson.BSONDocumentType
import java.nio.ByteOrder
import scala.annotation.tailrec

class WireProtocolFrame(maxSize: Int = BSONDocumentType.MaxSize) extends PipelineStage[PipelineContext, MongoClientWriteMessage, ByteString, MongoServerMessage, ByteString] {
  override def apply(ctx: PipelineContext) = new PipePair[MongoClientWriteMessage, ByteString, MongoServerMessage, ByteString] {
    var buffer = None: Option[ByteString]
    implicit val byteOrder = ByteOrder.LITTLE_ENDIAN

    /**
     * Extract completed frames from the given ByteString
     * @param bs
     * @param acc
     */
    @tailrec
    def extractFrames(bs: ByteString, acc: List[ByteString]) = ???

    /**
     * Commands (writes) transformed to the wire.
     */
    def commandPipeline: (MongoClientWriteMessage) => Iterable[this.type#Result] = ???

    /**
     * Reads from the wire.
     */
    def eventPipeline = { bs: ByteString =>
      /*
      state(IO.Chunk(bytes))
      log.debug("Decoding bytestream")
      val msg = for {
        lenBytes <- IO take(4) // 4 bytes, if aligned will be int32 length of following doc.
        len = lenBytes.iterator.getInt
        header <- IO take (16)
        frame <- IO take(len - 16 - 4) // length of total doc includes itself with BSON, so don't overflow.
      } yield MongoMessage(header.iterator, frame.iterator)    }
    */
      val data = if (buffer.isEmpty) bs else buffer.get ++ bs
      val (nb, frames) = extractFrames(data, Nil)
      buffer = nb
      frames match {
        case Nil        => Nil
        case one :: Nil => ctx.singleEvent(one)
        case many       => many.reverse map (Left(_))
      }
    }
  }
}
