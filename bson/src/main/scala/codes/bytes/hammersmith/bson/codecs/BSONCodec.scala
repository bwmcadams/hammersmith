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

// TODO - Check all our endianness corner cases becasue BSON is Crazytown Bananapants with endian consistency.
object BSONCodec extends StrictLogging {

  implicit val byteOrdering = ByteOrdering.LittleEndian

  val MaxBSONSize = 16 * 1024 * 1024

  val bsonSizeHeader = int32L.bounded(Interval(4, MaxBSONSize - 1))

  implicit val bsonCodec: Codec[(String, BSONType)] = {

    /**
      * BSON Double
      *
      * - Little Endian encoded
      * - 8 bytes (64-bit IEEE 754-2008 binary floating point)
      *
      * @group
      * @see http://bsonspec.org
      */
    val bsonDouble: Codec[BSONDouble] = doubleL.as[BSONDouble]
    val bsonString: Codec[BSONString] = utf8_32L.as[BSONString]

    val bsonDocument: Codec[BSONRawDocument] = lazily {
      // `as` doesn't seem to work with the vector piece
      vector(bsonCodec).xmap(
        { fields => BSONRawDocument(fields) },
        { doc => doc.primitiveValue }
      )
    }

    // TODO: Test me... all keys should be ints
    val bsonArray: Codec[BSONRawArray] = lazily {
      // `as` doesn't seem to work with the vector piece
      vector(bsonCodec).xmap(
        { entries => BSONRawArray.fromEntries(entries) },
        { arr => arr.primitiveValue }
      )
    }

    val bsonBinaryGeneric: Codec[BSONBinaryGeneric] = bytes.as[BSONBinaryGeneric]
      // bytes.xmap(bytes => BSONBinaryGeneric(bytes), bin => bin.bytes)

    val bsonBinaryFunction: Codec[BSONBinaryFunction] = bytes.as[BSONBinaryFunction]
      //bytes.xmap(bytes => BSONBinaryFunction(bytes), bin => bin.bytes)

    val bsonBinaryOld: Codec[BSONBinaryOld] = bytes.as[BSONBinaryOld]
      //bytes.xmap(bytes => BSONBinaryOld(bytes), bin => bin.bytes)

    val bsonBinaryOldUUID: Codec[BSONBinaryOldUUID] = BSONOldUUIDCodec

    val bsonBinaryUUID: Codec[BSONBinaryUUID] = BSONNewUUIDCodec

    val bsonBinaryMD5: Codec[BSONBinaryMD5] = bytes.as[BSONBinaryMD5]
      //bytes.xmap(bytes => BSONBinaryMD5(bytes), bin => bin.bytes)

    val bsonBinaryUserDefined: Codec[BSONBinaryUserDefined] = bytes.as[BSONBinaryUserDefined]
      //bytes.xmap(bytes => BSONBinaryUserDefined(bytes), bin => bin.bytes)

    val bsonBinary: Codec[BSONBinary] = lazily {
      // determine bson subtype
      discriminated[BSONBinary].by(uint8L).
        typecase(BSONBinaryGeneric.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryGeneric, 4)
        ).
        typecase(BSONBinaryFunction.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryFunction, 4)
        ).
        typecase(BSONBinaryOld.subTypeCode, // old binary had a double size for some reason
          variableSizeBytes(bsonSizeHeader, variableSizeBytes(bsonSizeHeader, bsonBinaryOld, 4))
        ).
        typecase(BSONBinaryOldUUID.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryOldUUID, 4)
        ).
        typecase(BSONBinaryUUID.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryUUID, 4)
        ).
        typecase(BSONBinaryMD5.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryMD5, 4)
        ).
        typecase(BSONBinaryUserDefined.subTypeCode,
          variableSizeBytes(bsonSizeHeader, bsonBinaryUserDefined, 4)
        )
    }

    /**
     * Some #s in OID are big endian, unlike rest of BSON is supposd to be (little end.).
     * [bwm: I love consistency!]
     *
     * Some of the pieces are simply broken into bytes here to let the deeper AST decode/encode
     * its crazytown bananapants quasi-ints.
     *
     * @see https://docs.mongodb.org/manual/reference/object-id/
     * @see https://github.com/mongodb/mongo/blob/master/src/mongo/bson/oid.h
     * @see http://stackoverflow.com/questions/23539486/endianess-of-parts-of-on-objectid-in-bson
     */
    val bsonObjectID: Codec[BSONObjectID] = (int32 ~ bytes(5) ~ uint32).xmap[BSONObjectID](
      { nums => BSONObjectID(nums._1._1, nums._1._2, nums._2) }, // todo - check me? double tupled?
      { bin: BSONObjectID => ((bin.timestamp, bin.machineID), bin.increment) }  // todo - check me? double tupled?
    )

    val bsonBoolean: Codec[BSONBoolean] = lazily {
      // determine bson subtype
      discriminated[BSONBoolean].by(uint8L).
        typecase(BSONBooleanTrue.subTypeCode, provide(BSONBooleanTrue)).
        typecase(BSONBooleanFalse.subTypeCode, provide(BSONBooleanFalse))
    }

    val bsonUTCDateTime: Codec[BSONUTCDateTime] = int64L.as[BSONUTCDateTime]

    // TODO - JSON spec says flags must be stored in alphabet order
    val bsonRegex: Codec[BSONRegex] = (cstring ~ cstring).xmap[BSONRegex](
      { case (pattern, options) => BSONRegex(pattern, options) },
      { case BSONRegex(pattern, options) => (pattern, options) }
    )

    val bsonDBPointer: Codec[BSONDBPointer] = (utf8 :: bsonObjectID).as[BSONDBPointer]

    val bsonJSCode: Codec[BSONJSCode] = (utf8).as[BSONJSCode]

    // Best to let the AST decide what it is , be it a Scala Symbol or what.
    val bsonSymbol: Codec[BSONSymbol] = (utf8).xmap[BSONSymbol](
      { str => BSONSymbol(str) },
      { case BSONSymbol(str) => str }
    )

    val bsonScopedJSCode: Codec[BSONScopedJSCode] = (utf8 :: bsonDocument).as[BSONScopedJSCode]

    val bsonInteger: Codec[BSONInteger] = (int32).as[BSONInteger]

    val bsonLong: Codec[BSONLong] = (int64L).as[BSONLong]

    val bsonTimestamp: Codec[BSONTimestamp] = (int64).as[BSONTimestamp]

    /**
      * Codec for decoding and encoding between BSON Documents, all valid inner types, and our internal AST
      *
      * @see http://bsonspec.org
      * @note Defined in the order they are in the BSON Spec , which is why the groupings are slightly odd
      */
    discriminated[(String, BSONType)].by(uint8L).
      typecase(BSONDouble.typeCode, cstring ~ bsonDouble).
      typecase(BSONString.typeCode, cstring ~ bsonString).
      typecase(BSONRawDocument.typeCode, cstring ~ bsonDocument).
      typecase(BSONRawArray.typeCode, cstring ~ bsonArray).
      typecase(BSONBinary.typeCode, cstring ~ variableSizeBytes(
        bsonSizeHeader, bsonBinary
      )).
      // this is really an asymmetric - we decode for posterity but shouldn't encode at AST Level
      typecase(BSONUndefined.typeCode, cstring ~ provide(BSONUndefined)).
      typecase(BSONObjectID.typeCode, cstring ~ bsonObjectID). // todo make me not suck
      typecase(BSONBoolean.typeCode, cstring ~ bsonBoolean).
      typecase(BSONUTCDateTime.typeCode, cstring ~ bsonUTCDateTime).
      typecase(BSONNull.typeCode, cstring ~ provide(BSONNull)).
      typecase(BSONRegex.typeCode, cstring ~ bsonRegex).
      typecase(BSONDBPointer.typeCode, cstring ~ bsonDBPointer).
      typecase(BSONJSCode.typeCode, cstring ~ bsonJSCode).
      typecase(BSONSymbol.typeCode, cstring ~ bsonSymbol).
      typecase(BSONScopedJSCode.typeCode, cstring ~ bsonScopedJSCode).
      typecase(BSONInteger.typeCode, cstring ~ bsonInteger).
      typecase(BSONMinKey.typeCode, cstring ~ provide(BSONMinKey)).
      typecase(BSONMaxKey.typeCode, cstring ~ provide(BSONMaxKey))

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
