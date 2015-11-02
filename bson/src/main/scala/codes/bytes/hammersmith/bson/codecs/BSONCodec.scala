package codes.bytes.hammersmith.bson.codecs

import java.util.UUID

import codes.bytes.hammersmith.bson.types._
import com.typesafe.scalalogging.StrictLogging
import scodec.{DecodeResult, Attempt, SizeBound, Codec}
import scodec.bits._
import scodec.codecs._
import scodec.interop.spire._
import spire.algebra._   // provides algebraic type classes
import spire.math._      // provides functions, types, and type classes
import spire.implicits._ // provides infix operators, instances and conversions

object BSONCodec extends StrictLogging {

  implicit val byteOrdering = ByteOrdering.LittleEndian

  val MaxBSONSize = 16 * 1024 * 1024

  val bsonSizeHeader = int32L.bounded(Interval(4, MaxBSONSize - 1))

  implicit val bsonCodec: Codec[(String, BSONType)] = {
/*
    implicit val bsonBinaryField: Codec[BSONBinary] =
      (
        BSONBinary.typeCodeConstant :: cstring ::
          variableSizeBytes(bsonSizeHeader, BSONBinaryCodec)
        ).dropUnits.as[BSONBinary]
*/

    val bsonDoubleCodec: Codec[BSONDouble] = doubleL.as[BSONDouble]
    val bsonStringCodec: Codec[BSONString] = utf8_32.as[BSONString]
    val bsonDocumentCodec: Codec[BSONRawDocument] = lazily {
      vector(bsonCodec)
        .xmap(fields => BSONRawDocument(fields), doc => doc.primitiveValue)
    }

    val bsonArrayCodec: Codec[BSONRawArray] = lazily {
      vector(bsonCodec)
        // TODO: Test me... all keys should be ints
        .xmap(entries => BSONRawArray.fromEntries(entries), arr => arr.primitiveValue)
    }

    val bsonBinaryGenericCodec: Codec[BSONBinaryGeneric] =
      bytes.xmap(bytes => BSONBinaryGeneric(bytes), bin => bin.bytes)

    val bsonBinaryFunctionCodec: Codec[BSONBinaryFunction] =
      bytes.xmap(bytes => BSONBinaryFunction(bytes), bin => bin.bytes)

    val bsonBinaryOldCodec: Codec[BSONBinaryOld] =
      bytes.xmap(bytes => BSONBinaryOld(bytes), bin => bin.bytes)

    val bsonBinaryOldUUIDCodec: Codec[BSONBinaryOldUUID] = BSONOldUUIDCodec

    val bsonBinaryUUIDCodec: Codec[BSONBinaryUUID] = BSONNewUUIDCodec

    val bsonBinaryMD5Codec: Codec[BSONBinaryMD5] =
      bytes.xmap(bytes => BSONBinaryMD5(bytes), bin => bin.bytes)

    val bsonBinaryUserDefinedCodec: Codec[BSONBinaryUserDefined] =
      bytes.xmap(bytes => BSONBinaryUserDefined(bytes), bin => bin.bytes)

    val bsonBinaryCodec: Codec[BSONBinary] = lazily {
      // determine bson subtype
      discriminated[BSONBinary].by(uint8L).
        typecase(BSONBinaryGeneric.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryGenericCodec, 4)
        ).
        typecase(BSONBinaryFunction.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryFunctionCodec, 4)
        ).
        typecase(BSONBinaryOld.subTypeCode, // old binary had a double size for some reason
          variableSizeBytes(bsonSizeHeader, variableSizeBytes(bsonSizeHeader, bsonBinaryOldCodec, 4))
        ).
        typecase(BSONBinaryOldUUID.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryOldUUIDCodec, 4)
        ).
        typecase(BSONBinaryUUID.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryUUIDCodec, 4)
        ).
        typecase(BSONBinaryMD5.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryMD5Codec, 4)
        ).
        typecase(BSONBinaryUserDefined.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryUserDefinedCodec, 4)
        )
    }

    // #s in OID are big endian. I love consistency.
    val bsonObjectIDCodec: Codec[BSONObjectID] =
      (int32 ~ int32 ~ int32).xmap[BSONObjectID](
        { nums => BSONObjectID(nums._1._1, nums._1._2, nums._2) }, // todo - check me? double tupled?
        { bin: BSONObjectID => ((bin.timestamp, bin.machineID), bin.increment) }  // todo - check me? double tupled?
      )


    discriminated[(String, BSONType)].by(uint8).
      typecase(BSONDouble.typeCode, cstring ~ bsonDoubleCodec).
      typecase(BSONString.typeCode, cstring ~ bsonStringCodec).
      typecase(BSONRawDocument.typeCode, cstring ~ bsonDocumentCodec).
      typecase(BSONRawArray.typeCode, cstring ~ bsonArrayCodec).
      typecase(BSONBinary.typeCode, cstring ~ variableSizeBytes(
        bsonSizeHeader, bsonBinaryCodec
      )).
      // this is really an asymmetric - we decode for posterity but shouldn't encode at AST Level
      typecase(BSONUndefined.typeCode, cstring ~ provide(BSONUndefined)).
      typecase(BSONObjectID.typeCode, cstring ~ bsonObjectIDCodec)
  }

}

/**
 * Old BSON UUIDs are little endian, new ones are big endian, though overall endianness is little in BSON
 *
 * TODO: At AST level, make sure we convert all Binary Old UUIDs to new standard Binary UUIDs
 */
object BSONOldUUIDCodec extends Codec[BSONBinaryOldUUID]{

  protected val mkUUID: (Long, Long) => BSONBinaryOldUUID = BSONBinaryOldUUID(_, _)

  override def encode(u: BSONBinaryOldUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.leastSignificant, u.mostSignificant)

  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

object BSONNewUUIDCodec extends Codec[BSONBinaryUUID]{

  protected val mkUUID: (Long, Long) => BSONBinaryUUID = BSONBinaryUUID(_, _)

  override def encode(u: BSONBinaryUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.mostSignificant, u.leastSignificant)

  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

/*object BSONBinaryCodec extends Codec[BSONBinary] {
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

  override def decode(bits: BitVector) = codec.decode(bits)

  override def encode(value: BSONBinary) = codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound

}*/
