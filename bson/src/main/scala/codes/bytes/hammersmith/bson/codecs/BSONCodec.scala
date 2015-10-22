package codes.bytes.hammersmith.bson.codecs

import codes.bytes.hammersmith.bson.types._
import com.typesafe.scalalogging.StrictLogging
import scodec.{DecodeResult, Attempt, SizeBound, Codec}
import scodec.bits._
import scodec.codecs._
import scodec.interop.spire._
import spire.math.Interval

object BSONCodec extends Codec[BSONType] with StrictLogging {

  val MaxBSONSize = 16 * 1024 * 1024


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

  val bsonDoubleCodec: Codec[BSONDouble] = double.as[BSONDouble]
  val bsonStringCodec: Codec[BSONString] = utf8_32.as[BSONString]
  val bsonDocumentCodec: Codec[BSONRawDocument] = lazily {
    vector(cstring ~ BSONCodec)
      .xmap(fields => BSONRawDocument(fields), doc => doc.primitiveValue)
  }

  val bsonArrayCodec: Codec[BSONRawArray] = lazily {
    vector(cstring ~ BSONCodec)
      // TODO: Test me... all keys should be ints
      .xmap(entries => BSONRawArray(entries), arr => arr.primitiveValue)
  }

  val bsonBinaryGenericCodec: Codec[BSONBinaryGeneric] =
    bytes.xmap( bytes => BSONBinaryGeneric(bytes), bin => bin.bytes )

  val bsonBinaryFunctionCodec: Codec[BSONBinaryFunction] =
    bytes.xmap( bytes => BSONBinaryFunction(bytes), bin => bin.bytes )

  val bsonBinaryOldCodec: Codec[BSONBinaryOld] =
    bytes.xmap( bytes => BSONBinaryOld(bytes), bin => bin.bytes )

  val bsonBinaryOldUUIDCodec: Codec[BSONBinaryOldUUID] =
    bytes.xmap( bytes => BSONBinaryOldUUID(bytes), bin => bin.bytes )

  val bsonBinaryCodec: Codec[BSONBinary] = lazily {
    // determine bson subtype
    discriminated[BSONBinary].by(uint8).
      typecase(BSONBinaryGeneric.subTypeCode,
        variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), bsonBinaryGenericCodec, 4)
      ).
      typecase(BSONBinaryFunction.subTypeCode,
        variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), bsonBinaryFunctionCodec, 4)
      ).
      typecase(BSONBinaryOld.subTypeCode,
        variableSizeBytes(int32.bounded(Interval(4, MaxBSONSize - 1)), bsonBinaryOldCodec, 4)
      ).
  }


  discriminated[(String, BSONType)].by(uint8).
    typecase(BSONDouble.typeCode, cstring ~ bsonDoubleCodec).
    typecase(BSONString.typeCode, cstring ~ bsonStringCodec).
    typecase(BSONRawDocument.typeCode, cstring ~ bsonDocumentCodec).
    typecase(BSONRawArray.typeCode, cstring ~ bsonArrayCodec).
    typecase(BSONBinary.typeCode, cstring ~ bsonBinaryCodec)
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
