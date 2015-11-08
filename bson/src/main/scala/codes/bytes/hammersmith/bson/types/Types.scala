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

package codes.bytes.hammersmith.bson.types

import codes.bytes.hammersmith.bson.ObjectID
import com.typesafe.scalalogging.StrictLogging
import scodec.bits._
import scodec.codecs._

/*
 * Roughly, this is an ADT representing an AST for BSON.
 * At least, first pass, we'll use this for Codec work in SCodec.
 * My hope is we can smooth over any performance of the AST Conversion.
 *
 * Sadly with base traits we can't seem to make AnyVal a doable thing.
 */

sealed trait BSONTypeCompanion {
  def typeCode: Byte

  def typeCodeConstant = constant(ByteVector(typeCode))
}

sealed trait BSONType {
  type Primitive

  def primitiveValue: Primitive
  // todo - the type class based support for working with converting to/from primitives flexibly
}


object BSONType {}


case object BSONEndOfDocument extends BSONType with BSONTypeCompanion {
  val typeCode: Byte = 0x00
  override type Primitive = Unit

  override def primitiveValue: BSONEndOfDocument.Primitive = ()
}

object BSONDouble extends BSONTypeCompanion {
  val typeCode: Byte = 0x01
}

case class BSONDouble(dbl: Double) extends BSONType {
  type Primitive = Double

  def primitiveValue = dbl
}

object BSONString extends BSONTypeCompanion {
  val typeCode: Byte = 0x02
}

// UTF8 string
case class BSONString(str: String) extends BSONType {
  type Primitive = String

  def primitiveValue = str
}

object BSONRawDocument extends BSONTypeCompanion {
  val typeCode: Byte = 0x03


}

// I don't see forcibly converting it into a map as having any value, given how the primitives are deconstructed
case class BSONRawDocument(entries: Vector[(String, BSONType)]) extends BSONType {
  type Primitive = Vector[(String, BSONType)]

  def primitiveValue = entries

}

object BSONRawArray extends BSONTypeCompanion with StrictLogging {
  val typeCode: Byte = 0x04

  def fromEntries(entries: Vector[(String, BSONType)]): BSONRawArray = {
    BSONRawArray(
      entries.map { case (k, v) =>
        val idx: Int = try { {
          k.toInt
        }
        } catch {
          case nfe: NumberFormatException =>
            logger.error("BSON Error: Typecode indicated array, but contains non-integer keys (indices)")
            throw new IllegalArgumentException("BSON Error: Typecode indicated array, but contains non-integer keys (indices)")
        }
        idx -> v
      } sortBy { case ((idx, value)) => idx } map (_._2)
    )
  }
}

case class BSONRawArray(entries: Vector[BSONType]) extends BSONType {
  // BSON Arrays are really docs with integer keys, but indexes (keys) are represented as strings...
  type Primitive = Vector[(String, BSONType)]

  def primitiveValue: Vector[(String, BSONType)] = entries.zipWithIndex.map { x =>
    x._2.toString -> x._1
  }
}

object BSONBinary extends BSONTypeCompanion {
  val typeCode: Byte = 0x05
  // todo - unapply to decompose BitVectors into subtypes

}


sealed trait BSONBinaryTypeCompanion extends BSONTypeCompanion {
  val typeCode: Byte = 0x05

  def subTypeCode: Byte

  def subTypeCodeConstant = constant(ByteVector(subTypeCode))
}

sealed trait BSONBinary extends BSONType


object BSONBinaryGeneric extends BSONBinaryTypeCompanion {
  val subTypeCode: Byte = 0x00

  /**
    * Legacy "just in case" support mostly for in-dev bridging before we kill Akka composers
    */
  def apply(bytes: Array[Byte]): BSONBinaryGeneric = apply(ByteVector(bytes))
}


/** Technically anything that ISN'T A user Defined (0x80+) would fit in generic...
  * TODO - Sort it out.
  */
case class BSONBinaryGeneric(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector

  def primitiveValue = bytes
}

object BSONBinaryFunction extends BSONBinaryTypeCompanion {
  val subTypeCode: Byte = 0x01

  /**
    * Legacy "just in case" support mostly for in-dev bridging before we kill Akka composers
    */
  def apply(bytes: Array[Byte]): BSONBinaryFunction = apply(ByteVector(bytes))
}

// note sure why anyone would need a binary function storage but the JS stuff Mongo has always supported is f-ing weird
case class BSONBinaryFunction(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector
  val primitiveValue = bytes
}

object BSONBinaryOld extends BSONBinaryTypeCompanion {
  // encoded with an int32 length at beginning
  def subTypeCode: Byte = 0x02

  /**
    * Legacy "just in case" support mostly for in-dev bridging before we kill Akka composers
    */
  def apply(bytes: Array[Byte]): BSONBinaryOld = apply(ByteVector(bytes))
}

case class BSONBinaryOld(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector
  val primitiveValue = bytes
}

object BSONBinaryOldUUID extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x03
}


// "Old" UUID format used little endianness which is NOT how UUIDs are encoded
final case class BSONBinaryOldUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinary {
  // for now, we'll represent as ourselves:  most significant & least significant (Not sure I can be circular here)
  type Primitive = BSONBinaryUUID
  val primitiveValue = BSONBinaryUUID(mostSignificant, leastSignificant)
}

object BSONBinaryUUID extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x04
}

// New UUIDs are Big Endian
final case class BSONBinaryUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinary {
  // for now, we'll represent as ourselves:  most significant & least significant (Not sure I can be circular here)
  type Primitive = BSONBinaryUUID
  val primitiveValue = this
}

object BSONBinaryMD5 extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x05

  /**
    * Legacy "just in case" support mostly for in-dev bridging before we kill Akka composers
    */
  def apply(bytes: Array[Byte]): BSONBinaryMD5 = apply(ByteVector(bytes))
}

// todo - should these use BitVectors from SCodec instead?
case class BSONBinaryMD5(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector

  def primitiveValue = bytes
}

object BSONBinaryUserDefined extends BSONBinaryTypeCompanion {
  // TODO - *technically* user defined can by >= 0x80 ... we need to sort that out.
  def subTypeCode: Byte = 0x80.toByte

  /**
    * Legacy "just in case" support mostly for in-dev bridging before we kill Akka composers
    */
  def apply(bytes: Array[Byte]): BSONBinaryUserDefined = apply(ByteVector(bytes))
}

case class BSONBinaryUserDefined(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector

  def primitiveValue = bytes
}


case object BSONUndefined extends BSONType with BSONTypeCompanion {
  val typeCode: Byte = 0x06
  // todo - how do we really wanna represent undef?
  type Primitive = None.type

  def primitiveValue = None
}


case object BSONObjectID extends BSONTypeCompanion {
  val typeCode: Byte = 0x07


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
case class BSONObjectID(bytes: ByteVector) extends BSONType {
  type Primitive = BSONObjectID

  val primitiveValue: Primitive = this

  // todo: to ObjectID
}

sealed trait BSONBooleanCompanion extends BSONTypeCompanion {
  val typeCode: Byte = BSONBoolean.typeCode

  def subTypeCode: Byte
}

sealed trait BSONBoolean extends BSONType {
  def booleanValue: Boolean
}


// todo - unfuck me
object BSONBoolean extends BSONTypeCompanion {
  val typeCode: Byte = 0x08
}


case object BSONBooleanTrue extends BSONBooleanCompanion with BSONBoolean {
  val subTypeCode: Byte = 0x00
  val booleanValue = true
  type Primitive = Boolean

  val primitiveValue = true
}

case object BSONBooleanFalse extends BSONBooleanCompanion with BSONBoolean {
  val subTypeCode: Byte = 0x01
  val booleanValue = false
  type Primitive = Boolean

  val primitiveValue = false
}

object BSONUTCDateTime extends BSONTypeCompanion {
  val typeCode: Byte = 0x09
}

case class BSONUTCDateTime(epoch: Long) extends BSONType {
  type Primitive = Long

  def primitiveValue = epoch
}

/**
  *
  * fucking bson null.
  */
case object BSONNull extends BSONType with BSONTypeCompanion {
  val typeCode: Byte = 0x0A
  // todo - how do we really wanna represent undef?
  type Primitive = None.type

  def primitiveValue = None
}

object BSONRegex extends BSONTypeCompanion {
  val typeCode: Byte = 0x0B
}

case class BSONRegex(regex: String, flags: String) extends BSONType {
  type Primitive = (String, String)

  // todo - verify valid flags both in and out
  def primitiveValue: Primitive = (regex, flags)

}

// TODO - Make sure we warn users that DBPointer is deprecated.
// TODO - Note that DBRefs aren't a BSONType but identified by a $ref field
object BSONDBPointer extends BSONTypeCompanion {
  val typeCode: Byte = 0x0C
}


// TODO - This is probably not a good marker as we'll have to reference it in our code. Find userspace ref deprecate.
//@deprecated("DBPointers have long been deprecated in BSON/MongoDB. Please use DBRefs instead.")
case class BSONDBPointer(ns: String, id: BSONObjectID) extends BSONType {
  type Primitive = (String, BSONObjectID)

  def primitiveValue: Primitive = (ns, id)
}

sealed trait BSONJSCodeBlockCompanion extends BSONTypeCompanion

sealed trait BSONJSCodeBlock extends BSONType

object BSONJSCode extends BSONJSCodeBlockCompanion {
  val typeCode: Byte = 0x0D
}

case class BSONJSCode(code: String) extends BSONJSCodeBlock {
  type Primitive = String

  def primitiveValue = code
}

// NOTE: BSONSymbols are deprecated as well, and Scala Symbols aren't the best...
object BSONSymbol extends BSONTypeCompanion {
  val typeCode: Byte = 0x0E
}

case class BSONSymbol(strValue: String) extends BSONType {
  type Primitive = Symbol

  def primitiveValue = Symbol(strValue)

}

object BSONScopedJSCode extends BSONJSCodeBlockCompanion {
  val typeCode: Byte = 0x0F
}

case class BSONScopedJSCode(code: String, scope: BSONRawDocument) extends BSONJSCodeBlock {
  type Primitive = String

  def primitiveValue = code
}

object BSONInteger extends BSONTypeCompanion {
  val typeCode: Byte = 0x10
}

case class BSONInteger(int: Int) extends BSONType {
  type Primitive = Int

  def primitiveValue = int
}

/** Special internal type for MongoDB Sharding, won't be representable as a JDK Type
  * TODO - Special BSON Type
  * */
object BSONTimestamp extends BSONTypeCompanion {
  val typeCode = 0x11.toByte
}

case class BSONTimestamp(increment: Int, time: Int) extends BSONType {
  type Primitive = (Int, Int)

  def primitiveValue = (increment, time)
}

object BSONLong extends BSONTypeCompanion {
  val typeCode: Byte = 0x12
}

case class BSONLong(long: Long) extends BSONType {
  type Primitive = Long

  def primitiveValue = long
}

sealed trait BSONKeyBoundaryCompanion extends BSONTypeCompanion

sealed trait BSONKeyBoundary extends BSONType

case object BSONMinKey extends BSONKeyBoundary with BSONKeyBoundaryCompanion {
  val typeCode: Byte = 0xFF.toByte
  // TODO - we need a sane representation of this type as a primitive
  type Primitive = None.type

  def primitiveValue = None
}

case object BSONMaxKey extends BSONKeyBoundary with BSONKeyBoundaryCompanion {
  val typeCode: Byte = 0x7F.toByte
  // TODO - we need a sane representation of this type as a primitive
  type Primitive = None.type

  def primitiveValue = None
}

// Currently no dereferencing support, etc. (not a fan anyway)
// not a BSON builtin type...
final case class DBRef(namespace: String, oid: ObjectID)

