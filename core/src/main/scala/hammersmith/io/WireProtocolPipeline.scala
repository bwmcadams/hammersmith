
package hammersmith.io

import akka.io._
import hammersmith.wire.MongoMessage
import akka.util.ByteString
import hammersmith.bson.BSONDocumentType
import java.nio.ByteOrder
import scala.annotation.tailrec
import hammersmith.util.Logging

class MongoFrameHandler(maxSize: Int = BSONDocumentType.MaxSize)
  extends SymmetricPipelineStage[PipelineContext, MongoMessage, ByteString]
  with Logging {
  override def apply(ctx: PipelineContext) = new SymmetricPipePair[MongoMessage, ByteString] {
    var buffer = None: Option[ByteString]
    implicit val byteOrder = ByteOrder.LITTLE_ENDIAN


    /**
     * Commands (writes) transformed to the wire.
     */
    def commandPipeline: (MongoMessage) => Iterable[this.type#Result] =  { msg: MongoMessage =>
      val bytes = msg.serialize()(maxSize)
      ctx.singleCommand(bytes)
    }


    @tailrec
    def extractFrames(bs: ByteString, acc: List[MongoMessage]): (Option[ByteString], Seq[MongoMessage]) = {
      if (bs.isEmpty) {
        (None, acc)
      } else if (bs.length < 4 /* header size */) {
        (Some(bs.compact), acc)
      } else {
        val len = bs.iterator.getInt // 4 bytes, if aligned will be int32 length of following doc
        require(len > 0 && len < maxSize,
                s"Received an invalid BSON frame size of '$len' bytes (Min: 'more than 4 bytes' Max: '$maxSize' bytes")

        //println(s"Decoding a ByteStream of '$len' bytes.")

        if (bs.length >= len) {
          val header = bs take 16 // Headers are exactly 16 bytes
          val frame = bs drop 16 take (len) /* subtract header;  length of total doc
                                                includes itself w/ BSON - don't overflow!!! */
          extractFrames(bs drop len, MongoMessage(header, frame) :: acc)
        } else {
          (Some(bs.compact), acc)
        }
      }
    }

    /**
     * Reads from the wire.
     * appends the received ByteString to the buffer (if any) and extracts the frames
     * from the result.
     */
    def eventPipeline = {
      bs: ByteString ⇒
      //println(s"event pipeline called w/ '$bs'")
      val data = if (buffer.isEmpty) bs else buffer.get ++ bs
      val (nb, frames) = extractFrames(data, Nil)
      buffer = nb

      frames match {
        case Nil => Nil
        case one :: Nil => ctx singleEvent one
        case many => many reverseMap (Left(_))
      }


    }
  }
}
