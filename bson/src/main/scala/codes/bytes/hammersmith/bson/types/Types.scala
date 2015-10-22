/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package codes.bytes.hammersmith.bson.types

import java.util.regex.Pattern

import codes.bytes.hammersmith.bson.ObjectID
import com.typesafe.scalalogging.StrictLogging
import scodec.Codec
import scodec.bits._
import scodec.codecs._

import scala.util.matching.Regex

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

object BSONType {
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
  def apply(entries: Vector[(String, BSONType)]): BSONRawArray = {
    BSONRawArray(
      entries.map { case (k, v) =>
        val idx: Int = try {
          k.toInt
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
  def primitiveValue = entries.zipWithIndex.map { x =>
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
}

/** Technically anything that ISN'T A user Defined (0x80+) would fit in generic...
  * TODO - Sort it out.
  * @param bytes
  */
case class BSONBinaryGeneric(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector
  def primitiveValue = bytes
}

object BSONBinaryFunction extends BSONBinaryTypeCompanion {
  val subTypeCode: Byte = 0x01
}

// note sure why anyone would need a binary function storage but the JS stuff Mongo has always supported is f-ing weird
case class BSONBinaryFunction(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector
  val primitiveValue = bytes
}

object BSONBinaryOld extends BSONBinaryTypeCompanion {
  // encoded with an int32 length at beginning
  def subTypeCode: Byte = 0x02
}

case class BSONBinaryOld(bytes: ByteVector) extends BSONBinary {
  type Primitive=  ByteVector
  val primitiveValue = bytes
}

object BSONBinaryOldUUID extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x03
}

// "Old" UUID format used little endianness which is NOT how UUIDs are encoded
final case class BSONBinaryOldUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinary {
  // for now, we'll represent as ourselves:  most significant & least significant (Not sure I can be circular here)
  type Primitive = BSONBinaryOldUUID
  val primitiveValue = this
}

object BSONBinaryUUID extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x04
}

final case class BSONBinaryUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinary {
  // for now, we'll represent as ourselves:  most significant & least significant (Not sure I can be circular here)
  type Primitive = BSONBinaryUUID
  val primitiveValue = this
}

object BSONBinaryMD5 extends BSONBinaryTypeCompanion {
  def subTypeCode: Byte = 0x05
}

// todo - should these use BitVectors from SCodec instead?
case class BSONBinaryMD5(bytes: ByteVector) extends BSONBinary {
  type Primitive = ByteVector
  def primitiveValue = bytes
}

object BSONBinaryUserDefined extends BSONBinaryTypeCompanion {
  // TODO - *technically* user defined can by >= 0x80 ... we need to sort that out.
  def subTypeCode: Byte = 0x80.toByte
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

case class BSONObjectID(timestamp: Int = (System.currentTimeMillis() / 1000).toInt,
                        machineID: Int = ObjectID.generatedMachineID,
                        increment: Int = ObjectID.nextIncrement(),
                        isNew: Boolean = true) extends BSONType {

  type Primitive = BSONObjectID

  def primitiveValue: Primitive = this

}

sealed trait BSONBooleanCompanion extends BSONTypeCompanion {
  val typeCode: Byte = 0x08
  def subTypeCode: Byte
}

sealed trait BSONBoolean extends BSONType {
  def booleanValue: Boolean
}

case object BSONBooleanTrue extends BSONBooleanCompanion with BSONType {
  def subTypeCode: Byte = 0x00
  val booleanValue = true
  type Primitive = Boolean

  def primitiveValue = true
}

case object BSONBooleanFalse extends BSONBooleanCompanion with BSONType {
  def subTypeCode: Byte = 0x01
  val booleanValue = false
  type Primitive = Boolean

  def primitiveValue = false
}

object BSONDateTime extends BSONTypeCompanion {
  def typeCode: Byte = 0x09
}

case class BSONDateTime(epoch: Long) extends BSONType {
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
  
  sealed case class BSONRegexFlag(javaCode: Int, charCode: Char) {

    // options == string, flags == int
    def apply(pattern: String, options: String): BSONRegex =
      BSONRegex("(?%s)%s".format(options, pattern))

  }

  def optionsFromFlags(flags: Int): String = {
    val buf = StringBuilder.newBuilder
    var _flags = flags

    for (flag <- Flags) {
      if ((_flags & flag.javaCode) > 0) {
        buf += flag.charCode
        _flags -= flag.javaCode
      }
    }
    buf.result()
  }

  val CanonEq = BSONRegexFlag(Pattern.CANON_EQ, 'c')
  val UnixLines = BSONRegexFlag(Pattern.UNIX_LINES, 'd')
  val Global = BSONRegexFlag(256, 'g')
  val CaseInsensitive = BSONRegexFlag(Pattern.CASE_INSENSITIVE, 'i')
  val Multiline = BSONRegexFlag(Pattern.MULTILINE, 'm')
  val DotAll = BSONRegexFlag(Pattern.DOTALL, 's')
  val Literal = BSONRegexFlag(Pattern.LITERAL, 't')
  val UnicodeCase = BSONRegexFlag(Pattern.UNICODE_CASE, 'u')
  val Comments = BSONRegexFlag(Pattern.COMMENTS, 'x')
  // this as an enumy list isn't ideal but it gets the job done cleanly for now.
  val Flags = List(CanonEq, UnixLines, Global, CaseInsensitive, Multiline, DotAll, Literal, UnicodeCase, Comments)

}

// also usable directly as its type... some of the ADT code will always be.
// todo - make sure pattern matching works cleanly...it should as we get unapply from scala.util.matching.Regex
case class BSONRegex(override val regex: String, groupNames: String*) extends Regex(regex, groupNames: _*) with BSONType {
  import BSONRegex._
  type Primitive = (String, String)
  // todo - verify valid flags both in and out
  def primitiveValue: Primitive = (regex, optionsFromFlags(flags))

  // TODO - Verify the flags from the generated regex are getting shoved in. (Have to compile into regex)

  //override val pattern = Pattern.compile(regex, flags)
  lazy val flags = pattern.flags
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

case class BSONSymbol(symbol: Symbol) extends BSONType {
  type Primitive = Symbol

  def primitiveValue = symbol
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
  **/
object BSONTimestamp extends BSONTypeCompanion {
  val typeCode = 0x11.toByte
}

case class BSONTimestamp(time: Int, increment: Int) extends BSONType {
  type Primitive = (Int, Int)
  def primitiveValue = (time, increment)
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

