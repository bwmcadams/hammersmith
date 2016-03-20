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

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.Charset

import codes.bytes.hammersmith.bson.types._
import com.typesafe.scalalogging.StrictLogging
import scodec.bits._
import scodec.codecs._
import scodec.interop.spire._
import scodec._
import spire.implicits._
import spire.math._

// provides infix operators, instances and conversions

// TODO - Check all our endianness corner cases because BSON is Crazytown Bananapants with endian consistency.
// TODO - Finish ScalaDoc
object BSONCodec extends StrictLogging {

  implicit val byteOrdering = ByteOrdering.LittleEndian

  val MaxBSONSize = 16 * 1024 * 1024

  val BSONPaddingBytes = 4
  val BSONPaddingBits = BSONPaddingBytes * 8

  val Nul = constant(hex"00")

  val bsonSizeBytesHeaderCodec = logToStdOut(int32L.bounded(Interval(4, MaxBSONSize)), "!!! HEADER: ")

  implicit val bsonDocumentCodec: Codec[BSONRawDocument] =
    new BSONDocumentCodec(bsonFieldCodec)

  implicit lazy val bsonArrayCodec: Codec[BSONRawArray] =
    new BSONArrayCodec(bsonFieldCodec)

  implicit lazy val bsonFieldCodec: Codec[(String, BSONType)] = lazily {

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

    val bsonUTF8StringBase = variableSizeBytes(int32L, string(Charset.forName("UTF-8")), 1)

    /**
     * BSON String
     *
     * - Little Endian encoded
     * - UTF8 with int32 length header
     *
     * TODO: From BSONSpec:
     * "Zero or more modified UTF-8 encoded characters followed by '\x00'.
     *  The (byte*) MUST NOT contain '\x00', hence it is not full UTF-8."
     *
     * @group bsonTypeCodec
     * @see http://bsonspec.org
     */
    val bsonString: Codec[BSONString] =
      bsonUTF8StringBase.as[BSONString] <~ Nul


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
          variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryGeneric, 4)).
          typecase(BSONBinaryFunction.subTypeCode,
            variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryFunction, 4)).
            typecase(BSONBinaryOld.subTypeCode, // old binary had a double size for some reason
              variableSizeBytes(bsonSizeBytesHeaderCodec, variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryOld, 4))).
              typecase(BSONBinaryOldUUID.subTypeCode,
                variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryOldUUID, 4)).
                typecase(BSONBinaryUUID.subTypeCode,
                  variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryUUID, 4)).
                  typecase(BSONBinaryMD5.subTypeCode,
                    variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryMD5, 4)).
                    typecase(BSONBinaryUserDefined.subTypeCode,
                      variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinaryUserDefined, 4))
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

    // TODO Changed in version 2.0: BSON Date type is signed. [2] Negative values represent dates before 1970.
    val bsonUTCDateTime: Codec[BSONUTCDateTime] = int64L.as[BSONUTCDateTime]

    // TODO - BSON spec says flags must be stored in alphabet order
    val bsonRegex: Codec[BSONRegex] = (cstring :: cstring).as[BSONRegex]

    val bsonDBPointer: Codec[BSONDBPointer] = (bsonUTF8StringBase :: bsonObjectID).as[BSONDBPointer]

    val bsonJSCode: Codec[BSONJSCode] = (bsonUTF8StringBase).as[BSONJSCode]

    val bsonSymbol: Codec[BSONSymbol] = (bsonUTF8StringBase).as[BSONSymbol]

    val bsonScopedJSCode: Codec[BSONScopedJSCode] = (bsonUTF8StringBase :: bsonDocumentCodec).as[BSONScopedJSCode]

    val bsonInteger: Codec[BSONInteger] = (int32L).as[BSONInteger]

    val bsonLong: Codec[BSONLong] = (int64L).as[BSONLong]

    val bsonTimestamp: Codec[BSONTimestamp] = (int32L :: int32L).as[BSONTimestamp]

    /**
     * Codec for decoding and encoding between BSON Documents, all valid inner types, and our internal AST
     *
     * @see http://bsonspec.org
     * @note Defined in the order they are in the BSON Spec , which is why the groupings are slightly odd
     */
    logToStdOut(
      discriminated[(String, BSONType)].
        by(uint8L).
        subcaseO(BSONDouble.typeCode) {
          case (nme, fld: BSONDouble) ⇒ Some(nme → fld)
          case _ ⇒ None
        } (cstring ~ bsonDouble).
        subcaseO(BSONString.typeCode) {
          case (nme, fld: BSONString) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonString).
        subcaseO(BSONRawDocument.typeCode) {
          case (nme, fld: BSONRawDocument) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonDocumentCodec).
        subcaseO(BSONRawArray.typeCode) {
          case (nme, fld: BSONRawArray) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonArrayCodec).
        subcaseO(BSONBinary.typeCode) {
          case (nme, fld: BSONBinary) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ variableSizeBytes(bsonSizeBytesHeaderCodec, bsonBinary)).
        // this is really an asymmetric - we decode for posterity but shouldn't encode at AST Level
        subcaseO(BSONUndefined.typeCode) {
          case (nme, BSONUndefined) ⇒ Some(nme → BSONUndefined)
          case _ ⇒ None
        }(cstring ~ provide(BSONUndefined)).
        subcaseO(BSONObjectID.typeCode) {
          case (nme, fld: BSONObjectID) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonObjectID).
        subcaseO(BSONBoolean.typeCode) {
          case (nme, fld: BSONBoolean) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonBoolean).
        subcaseO(BSONUTCDateTime.typeCode) {
          case (nme, fld: BSONUTCDateTime) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonUTCDateTime).
        subcaseO(BSONNull.typeCode) {
          case (nme, BSONNull) ⇒ Some(nme → BSONNull)
          case _ ⇒ None
        }(cstring ~ provide(BSONNull)).
        subcaseO(BSONRegex.typeCode) {
          case (nme, fld: BSONRegex) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonRegex).
        subcaseO(BSONDBPointer.typeCode) {
          case (nme, fld: BSONDBPointer) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonDBPointer).
        subcaseO(BSONJSCode.typeCode) {
          case (nme, fld: BSONJSCode) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonJSCode).
        subcaseO(BSONSymbol.typeCode) {
          case (nme, fld: BSONSymbol) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonSymbol).
        subcaseO(BSONScopedJSCode.typeCode) {
          case (nme, fld: BSONScopedJSCode) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonScopedJSCode).
        subcaseO(BSONInteger.typeCode) {
          case (nme, fld: BSONInteger) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonInteger).
        subcaseO(BSONTimestamp.typeCode) {
          case (nme, fld: BSONTimestamp) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonTimestamp).
        subcaseO(BSONLong.typeCode) {
          case (nme, fld: BSONLong) ⇒ Some(nme → fld)
          case _ ⇒ None
        }(cstring ~ bsonLong).
        subcaseO(255 /* because jvm bytes are signed, 0xff can't be byteified to 255 */ ) {
          case (nme, BSONMinKey) ⇒ Some(nme → BSONMinKey)
          case _ ⇒ None
        }(cstring ~ provide(BSONMinKey)).
        subcaseO(BSONMaxKey.typeCode) {
          case (nme, BSONMaxKey) ⇒ Some(nme → BSONMaxKey)
          case _ ⇒ None
        }(cstring ~ provide(BSONMaxKey)).
        subcaseO(BSONEndOfDocument.typeCode) {
          case (nme, BSONEndOfDocument) ⇒ Some(nme → BSONEndOfDocument)
          case _ ⇒ None
        }(provide("") ~ provide(BSONEndOfDocument)),
      "#\t ")
  }

  //def encode(d: BSONRawDocument) = bsonCodec.
  // TODO: Return to attempt and drop option
  def decode(m: BitVector): Option[BSONRawDocument] = bsonDocumentCodec.decode(m).map { dr ⇒
    dr.value
  }.toOption

  // todo - log errors
  def encode(doc: BSONRawDocument): Option[BitVector] =
    bsonDocumentCodec.encode(doc).toOption
}

/**
 * Old BSON UUIDs are little endian, new ones are big endian, though overall endianness is little in BSON
 *
 * TODO: At AST level, make sure we convert all Binary Old UUIDs to new standard Binary UUIDs
 */
object BSONOldUUIDCodec extends Codec[BSONBinaryOldUUID] {

  protected val mkUUID: (Long, Long) ⇒ BSONBinaryOldUUID = BSONBinaryOldUUID(_, _)

  override def encode(u: BSONBinaryOldUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.leastSignificant, u.mostSignificant)

  // todo - decoding in right order/endianness?
  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

object BSONNewUUIDCodec extends Codec[BSONBinaryUUID] {

  protected val mkUUID: (Long, Long) ⇒ BSONBinaryUUID = BSONBinaryUUID(_, _)

  override def encode(u: BSONBinaryUUID): Attempt[BitVector] =
    Codec.encodeBoth(int64, int64)(u.mostSignificant, u.leastSignificant)

  // todo - decoding in right order/endianness?
  override def decode(bits: BitVector) =
    Codec.decodeBothCombine(int64, int64)(bits)(mkUUID)

  override val sizeBound = int64.sizeBound * 2L

  override def toString = "uuid"
}

/**
 * BSON Document
 *
 * This is, fundamentally, the core type of BSON. But BSON Docs can contain docs as fields;
 * note we recurse against the parent codec.
 *
 * ____Implementation Note____
 * OK so here's the deal. Each outer or embedded doc/array is:
 * - an int32 containing # of bytes in doc, this includes itself (pad 4)
 * - a series of "elements" (vector), decoded by bsonFieldCodec. Element made up of:
 *   + single byte "type code"
 *   + a cstring
 *   + a type specific data block
 *   ~ There is *NO* defined boundary to say "this field is $x long"
 * - a 0x00 (NUL) terminator
 *
 * we have to be careful about how we decode so that we do embedded docs/arrays right.
 * Outer docs in my experience have been easier to handle.
 * My preference would be to scan ahead, check for NUL... but this is highly inefficient
 * in most high performance network layers. I think we can simply define the codec as
 * a vector of (expected bytes - 1 byte [nul]).
 * Sadly, the existing vector / vectorOfN / etc can't handle safely so we've whipped one up ourselves.
 * Also need to make sure we encode the NUL at the end too.
 *
 * @group bsonTypeCodec
 * @see http://bsonspec.org
 * @param fieldCodec A Codec capable of decoding/encoding pairs of field name & bson type
 *
 * todo - abstract / share code between Document and Array
 *
 */
final class BSONDocumentCodec(fieldCodec: Codec[(String, BSONType)]) extends Codec[BSONRawDocument] {

  import BSONCodec._

  private val decoder = variableSizeBytes(bsonSizeBytesHeaderCodec, vector(fieldCodec), 4)

  private val encoder = variableSizeBytes(bsonSizeBytesHeaderCodec, vector(fieldCodec), 4)

  // sizeBound is in Bits...
  def sizeBound = bsonSizeBytesHeaderCodec.sizeBound.atLeast

  // todo - make sure we're sticking in the Nul *AND* the proper length
  def encode(bsonDoc: BSONRawDocument) = {
    val encoded = encoder.encode(bsonDoc.entries)
    encoded.map { x =>
      val sz = x.size.toInt + 4
      val b = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(sz)
      BitVector(b) ++ x
    }
  }

  def decode(buffer: BitVector) = {
    decoder.decode(buffer).map { r ⇒
      DecodeResult(BSONRawDocument(
        r.value.filterNot { case (k, v) ⇒ v == BSONEndOfDocument }),
        r.remainder)
    }
  }

  override def toString = s"bsonDocument($fieldCodec)"

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
final class BSONArrayCodec(fieldCodec: Codec[(String, BSONType)]) extends Codec[BSONRawArray] {

  import BSONCodec._

  private val decoder = variableSizeBytes(bsonSizeBytesHeaderCodec, vector(fieldCodec), 4)

  // sizeBound is in Bits...
  def sizeBound = bsonSizeBytesHeaderCodec.sizeBound.atLeast

  // todo - make sure we're sticking in the Nul *AND* the proper length
  def encode(bsonArray: BSONRawArray) =
    Encoder.encodeSeq(fieldCodec <~ Nul)(
      bsonArray.entries.zipWithIndex.map { case (e, i) ⇒ i.toString → e })

  def decode(buffer: BitVector) = {
    decoder.decode(buffer).map { r ⇒
      DecodeResult(BSONRawArray(
        r.value.filterNot {
          case (k, v) ⇒ v == BSONEndOfDocument
        } map {
          case (k, v) ⇒ v
        }),
        r.remainder)
    }
  }

  override def toString = s"bsonDocument($fieldCodec)"

}
