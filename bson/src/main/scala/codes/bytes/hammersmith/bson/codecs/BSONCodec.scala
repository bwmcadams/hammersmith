/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
package codes.bytes.hammersmith.bson.codecs

import codes.bytes.hammersmith.bson.types._
import com.typesafe.scalalogging.StrictLogging
import scodec.bits._
import scodec.codecs._
import scodec.interop.spire._
import scodec.{Attempt, Codec}
import spire.implicits._
import spire.math._ // provides infix operators, instances and conversions

// TODO - Check all our endianness corner cases because BSON is Crazytown Bananapants with endian consistency.
// TODO - Finish ScalaDoc
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
      * @group bsonTypeCodec
      * @see http://bsonspec.org
      */
    val bsonDouble: Codec[BSONDouble] = doubleL.as[BSONDouble]

    /**
      * BSON String
      *
      * - Little Endian encoded
      * - UTF8 with int32 length header
      *
      * @group bsonTypeCodec
      * @see http://bsonspec.org
      */
    val bsonString: Codec[BSONString] = utf8_32L.as[BSONString]

    /**
      * BSON Document
      *
      * This is, fundamentally, the core type of BSON. But BSON Docs can contain docs as fields;
      * note we recurse against the parent codec.
      *
      * @group bsonTypeCodec
      * @see http://bsonspec.org
      */
    val bsonDocument: Codec[BSONRawDocument] = lazily {
      vector(bsonCodec).xmap(
        { fields => BSONRawDocument(fields) }, { doc => doc.primitiveValue }
      )
    }


    /**
      * BSON Array
      *
      * Technically a standard BSON Document with integer keys
      * - indexed from 0
      * - sequential
      *
      * TODO: Test me... all keys should be ints
      *
      * @group bsonTypeCodec
      * @see http://bsonspec.org
      */
    val bsonArray: Codec[BSONRawArray] = lazily {
      vector(bsonCodec).xmap(
        { entries => BSONRawArray.fromEntries(entries) }, { arr => arr.primitiveValue }
      )
    }

    /**
      * BSON Binary Subtype for “Generic” binary.
      */
    val bsonBinaryGeneric: Codec[BSONBinaryGeneric] = bytes.as[BSONBinaryGeneric]

    val bsonBinaryFunction: Codec[BSONBinaryFunction] = bytes.as[BSONBinaryFunction]

    val bsonBinaryOld: Codec[BSONBinaryOld] = bytes.as[BSONBinaryOld]

    val bsonBinaryOldUUID: Codec[BSONBinaryOldUUID] = BSONOldUUIDCodec

    val bsonBinaryUUID: Codec[BSONBinaryUUID] = BSONNewUUIDCodec

    val bsonBinaryMD5: Codec[BSONBinaryMD5] = bytes.as[BSONBinaryMD5]

    val bsonBinaryUserDefined: Codec[BSONBinaryUserDefined] = bytes.as[BSONBinaryUserDefined]

    /**
      * Binary data, with special subtypes.
      *
      * - Int32 prefix of length
      * - Byte Array
      *
      * TODO: Support full range of user subtypes, which can be 128-255, rather than just 255
      *
      * @group bsonTypeCodec
      * @see http://bsonspec.org
      */
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
      * The pieces are simply weird here including 3 byte unsigned, big endian integers
      * its crazytown bananapants quasi-ints.
      *
      * From Mongo Server Doc:
      * {{{
      *               4 byte timestamp    5 byte process unique   3 byte counter
      *             |<----------------->|<---------------------->|<------------->
      * OID layout: [----|----|----|----|----|----|----|----|----|----|----|----]
      *             0                   4                   8                   12
      * }}}
      *
      * - Timestamp is a 4 byte signed int32, encoded as Big Endian
      * - “Process Unique” is 2 sections
      * + Machine Identifier is an int32 composed of 3 Low Order (Per Spec: Little Endian Bytes)
      * + Process Identifier is a short (composed of 2 Low Order (Per Spec: Little Endian Bytes)
      * % The Java Driver seems to ignore this and stores process-unique in BE.
      * % Other drivers like Python seem to ignore spec, too.
      * - Counter / Increment is a 3 byte counter to prevent races against tsp/mid/pid
      *
      * @note There's no value I know of in exposing the actual pieces of an ObjectID in user code... so we
      *       read & write as the raw binary value. Generation of new OIDs will hide those pieces too.
      *
      * @see https://docs.mongodb.org/manual/reference/object-id/
      * @see https://github.com/mongodb/mongo/blob/master/src/mongo/bson/oid.h
      * @see http://stackoverflow.com/questions/23539486/endianess-of-parts-of-on-objectid-in-bson
      */
    val bsonObjectID: Codec[BSONObjectID] = (bytes(12)).as[BSONObjectID]

    val bsonBoolean: Codec[BSONBoolean] = lazily {
      // determine bson subtype
      discriminated[BSONBoolean].by(uint8L).
        typecase(BSONBooleanTrue.subTypeCode, provide(BSONBooleanTrue)).
        typecase(BSONBooleanFalse.subTypeCode, provide(BSONBooleanFalse))
    }

    val bsonUTCDateTime: Codec[BSONUTCDateTime] = int64L.as[BSONUTCDateTime]

    // TODO - BSON spec says flags must be stored in alphabet order
    val bsonRegex: Codec[BSONRegex] = (cstring :: cstring).as[BSONRegex]

    val bsonDBPointer: Codec[BSONDBPointer] = (utf8 :: bsonObjectID).as[BSONDBPointer]

    val bsonJSCode: Codec[BSONJSCode] = (utf8).as[BSONJSCode]

    val bsonSymbol: Codec[BSONSymbol] = (utf8).as[BSONSymbol]

    val bsonScopedJSCode: Codec[BSONScopedJSCode] = (utf8 :: bsonDocument).as[BSONScopedJSCode]

    val bsonInteger: Codec[BSONInteger] = (int32).as[BSONInteger]

    val bsonLong: Codec[BSONLong] = (int64L).as[BSONLong]

    val bsonTimestamp: Codec[BSONTimestamp] = (int32L :: int32L).as[BSONTimestamp]

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
      typecase(BSONObjectID.typeCode, cstring ~ bsonObjectID).
      typecase(BSONBoolean.typeCode, cstring ~ bsonBoolean).
      typecase(BSONUTCDateTime.typeCode, cstring ~ bsonUTCDateTime).
      typecase(BSONNull.typeCode, cstring ~ provide(BSONNull)).
      typecase(BSONRegex.typeCode, cstring ~ bsonRegex).
      typecase(BSONDBPointer.typeCode, cstring ~ bsonDBPointer).
      typecase(BSONJSCode.typeCode, cstring ~ bsonJSCode).
      typecase(BSONSymbol.typeCode, cstring ~ bsonSymbol).
      typecase(BSONScopedJSCode.typeCode, cstring ~ bsonScopedJSCode).
      typecase(BSONInteger.typeCode, cstring ~ bsonInteger).
      typecase(BSONTimestamp.typeCode, cstring ~ bsonTimestamp).
      typecase(BSONLong.typeCode, cstring ~ bsonLong).
      typecase(BSONMinKey.typeCode, cstring ~ provide(BSONMinKey)).
      typecase(BSONMaxKey.typeCode, cstring ~ provide(BSONMaxKey))

  }

}

/**
  * Old BSON UUIDs are little endian, new ones are big endian, though overall endianness is little in BSON
  *
  * TODO: At AST level, make sure we convert all Binary Old UUIDs to new standard Binary UUIDs
  */
object BSONOldUUIDCodec extends Codec[BSONBinaryOldUUID] {

  protected val mkUUID: (Long, Long) => BSONBinaryOldUUID = BSONBinaryOldUUID(_, _)

  override def encode(u: BSONBinaryOldUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.leastSignificant, u.mostSignificant)

  // todo - decoding in right order/endianness?
  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

object BSONNewUUIDCodec extends Codec[BSONBinaryUUID] {

  protected val mkUUID: (Long, Long) => BSONBinaryUUID = BSONBinaryUUID(_, _)

  override def encode(u: BSONBinaryUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.mostSignificant, u.leastSignificant)

  // todo - decoding in right order/endianness?
  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

