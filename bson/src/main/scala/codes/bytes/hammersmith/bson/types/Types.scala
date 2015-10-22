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
import scodec.bits._

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
}

sealed trait BSONType {
  type Primitive
  def primitiveValue: Primitive
  // todo - the type class based support for working with converting to/from primitives flexibly
}

object BSONDouble extends BSONTypeCompanion {
  val typeCode: Byte = 0x01
}

case class BSONDouble(primitive: Double) extends BSONType {
  type Primitive = Double
  def primitiveValue = primitive
}

object BSONString extends BSONTypeCompanion {
  val typeCode: Byte = 0x02
}

// UTF8 string
case class BSONString(primitive: String) extends BSONType {
  type Primitive = String
  def primitiveValue = primitive
}

object BSONDocument extends BSONTypeCompanion {
  val typeCode: Byte = 0x03
}

// I don't see forcibly converting it into a map as having any value, given how the primitives are deconstructed
case class BSONDocument(primitive: Seq[(String, Any)]) extends BSONType {
  type Primitive = Seq[(String, Any)]
  def primitiveValue = primitive
}

object BSONArray extends BSONTypeCompanion {
  val typeCode: Byte = 0x04
}

case class BSONArray(primitive: Seq[Any]) extends BSONType {
  type Primitive = Seq[Any]
  def primitiveValue = primitive
}

object BSONBinary extends BSONTypeCompanion {
  val typeCode: Byte = 0x05
  // todo - unapply to decompose BitVectors into subtypes

}


sealed trait BSONBinaryTypeCompanion extends BSONTypeCompanion {
  val typeCode: Byte = 0x05
  def subTypeCode: Byte
}

sealed trait BSONBinary extends BSONType


object BSONBinaryGeneric extends BSONBinaryTypeCompanion {
  val subTypeCode: Byte = 0x00
}

/** Technically anything that ISN'T A user Defined (0x80+) would fit in generic...
  * TODO - Sort it out.
  * @param bytes
  */
case class BSONBinaryGeneric(bytes: Array[Byte]) extends BSONBinary {
  type Primitive = Array[Byte]
  def primitiveValue = bytes
}

object BSONBinaryFunction extends BSONBinaryTypeCompanion {
  val subTypeCode: Byte = 0x01
}

// note sure why anyone would need a binary function storage but the JS stuff Mongo has always supported is f-ing weird
case class BSONBinaryFunction(bytes: Array[Byte]) extends BSONBinary {
  type Primitive = Array[Byte]
  val primitiveValue = bytes
}

object BSONBinaryOld extends BSONBinaryTypeCompanion {
  // encoded with an int32 length at beginning
  def subTypeCode: Byte = 0x02
}

case class BSONBinaryOld(bytes: Array[Byte]) extends BSONBinary {
  type Primitive=  Array[Byte]
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
case class BSONBinaryMD5(bytes: Array[Byte]) extends BSONBinary {
  type Primitive = Array[Byte]
  def primitiveValue = bytes
}

object BSONBinaryUserDefined extends BSONBinaryTypeCompanion {
  // TODO - *technically* user defined can by >= 0x80 ... we need to sort that out.
  def subTypeCode: Byte = 0x80.toByte
}

case class BSONBinaryUserDefined(bytes: Array[Byte]) extends BSONBinary {
  type Primitive = Array[Byte]
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

  override type Primitive = BSONObjectID

  override def primitiveValue: Primitive = this

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
  override type Primitive = (String, String)
  // todo - verify valid flags both in and out
  override def primitiveValue: Primitive = (regex, optionsFromFlags(flags))

  // TODO - Verify the flags from the generated regex are getting shoved in. (Have to compile into regex)

  //override val pattern = Pattern.compile(regex, flags)
  lazy val flags = pattern.flags
}


// Things that are mostly hard representations
sealed trait SpecialBSONValue
/** BSON Min Key and Max Key represent special internal types for Sharding */
case object BSONMinKey extends SpecialBSONValue
case object BSONMaxKey extends SpecialBSONValue
/** The dumbest types I've ever seen on earth */
case object BSONNull extends SpecialBSONValue
case object BSONUndef extends SpecialBSONValue

// Currently no dereferencing support, etc. (not a fan anyway)
final case class DBRef(namespace: String, oid: ObjectID)

sealed trait BSONCodeBlock
// needs a document for scope
final case class BSONCodeWScope(code: String, scope: Map[String, Any]) extends BSONCodeBlock
final case class BSONCode(code: String) extends BSONCodeBlock
final case class BSONTimestamp(time: Int, increment: Int)
