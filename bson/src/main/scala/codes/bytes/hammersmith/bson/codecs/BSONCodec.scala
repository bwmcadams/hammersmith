package codes.bytes.hammersmith.bson.codecs

import codes.bytes.hammersmith.bson.types._
import scodec.{DecodeResult, Attempt, SizeBound, Codec}
import scodec.bits._
import scodec.codecs._
import scodec.interop.spire._
import spire.math.Interval

object BSONCodec extends Codec[BSONType] {

  val MaxBSONSize = 16 * 1024 * 1024

  implicit val doubleField: Codec[(String, BSONDouble)] =
    (BSONDouble.typeCodeConstant :: cstring :: double).dropUnits

  implicit val utf8StringField: Codec[(String, String)] =
    (BSONString.typeCodeConstant :: cstring :: utf8).dropUnits.as[(String, String)]

  // todo - internal type vs. external
  implicit val bsonDocument: Codec[BSONRawDocument] =
    (int32 :: variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), BSONCodec, 4)).dropUnits.as[BSONRawDocument]

  // todo - internal type vs. external
  implicit val bsonArray: Codec[BSONRawArray] =
    (int32 :: variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), BSONCodec, 4)).dropUnits.as[BSONRawArray]

  implicit val bsonBinaryField: Codec[BSONBinary] =
    (BSONBinary.typeCodeConstant :: cstring ::
      variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), BSONBinaryCodec)
    ).dropUnits.as[BSONBinary]

  private val codec: Codec[BSONType] =
    scodec.codecs.lazily { Codec.coproduct[BSONType].choice }

  override def decode(bits: BitVector) = codec.decode(bits)

  override def encode(value: BSONType) = codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound
}

object BSONBinaryCodec extends Codec[BSONBinary] {
  implicit val bsonBinaryGeneric: Codec[BSONBinaryGeneric] =
    (BSONBinaryGeneric.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryGeneric]

  implicit val bsonBinaryFunction: Codec[BSONBinaryFunction] =
    (BSONBinaryFunction.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryFunction]

  implicit val bsonBinaryOld: Codec[BSONBinaryOld] =
    (BSONBinaryOld.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryOld]

  implicit val bsonBinaryOldUUID: Codec[BSONBinaryOldUUID] =
    (BSONBinaryOldUUID.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryOldUUID]

  implicit val bsonBinaryUUID: Codec[BSONBinaryUUID] =
    (BSONBinaryUUID.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryUUID]

  implicit val bsonBinaryMD5: Codec[BSONBinaryMD5] =
    (BSONBinaryMD5.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryMD5]

  implicit val bsonBinaryUserDefined: Codec[BSONBinaryUserDefined] =
    (BSONBinaryUserDefined.subTypeCodeConstant :: bytes).dropUnits.as[BSONBinaryUserDefined]

  private val codec: Codec[BSONBinary] =
    scodec.codecs.lazily { Codec.coproduct[BSONBinary].choice }

  override def decode(bits: BitVector) = codec.decode(bits)

  override def encode(value: BSONBinary) = codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound

}
