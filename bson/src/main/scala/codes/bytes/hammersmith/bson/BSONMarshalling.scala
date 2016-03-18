/*
 * Copyright (c) 2011-2016 Brendan McAdams <http://bytes.codes>
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

package codes.bytes.hammersmith.bson

import codes.bytes.hammersmith.bson.primitive.MongoTimestamp
import codes.bytes.hammersmith.bson.types._

import scala.util.matching.Regex


sealed trait BSONMarshallingBase[T] {
  type BSONPrimitiveType <: BSONType
}

trait BSONDeserializer[T] extends BSONMarshallingBase[T] {
  def toNative(bsonType: BSONPrimitiveType): T
}

trait BSONSerializer[T] extends BSONMarshallingBase[T] {
  def toBSONType(native: T): BSONPrimitiveType
}

trait BSONBooleanSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONBoolean
}

trait BSONBooleanDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONBoolean
}

trait BSONDoubleSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONDouble
}

trait BSONDoubleDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONDouble
}

trait BSONIntegerSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONInteger
}

trait BSONIntegerDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONInteger
}

trait BSONLongSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONLong
}

trait BSONLongDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONLong
}

sealed trait BSONRegexFlags {

  import java.util.regex.Pattern

  final case class Flag (
    javaCode: Int,
    charCode: Char
  )


  val CanonEq = Flag(Pattern.CANON_EQ, 'c')
  val UnixLines = Flag(Pattern.UNIX_LINES, 'd')
  val Global = Flag(256, 'g')
  val CaseInsensitive = Flag(Pattern.CASE_INSENSITIVE, 'i')
  val Multiline = Flag(Pattern.MULTILINE, 'm')
  val DotAll = Flag(Pattern.DOTALL, 's')
  val Literal = Flag(Pattern.LITERAL, 't')
  val UnicodeCase = Flag(Pattern.UNICODE_CASE, 'u')
  val Comments = Flag(Pattern.COMMENTS, 'x')
  val Flags = Vector(CanonEq, UnixLines, Global, CaseInsensitive, Multiline, DotAll, Literal, UnicodeCase, Comments)

}

trait BSONRegexSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONRegex
}

trait BSONRegexDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONRegex
}

trait BSONStringSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONString
}

trait BSONStringDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONString
}

trait BSONSymbolSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONSymbol
}

trait BSONSymbolDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONSymbol
}

trait BSONTimestampSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONTimestamp
}

trait BSONTimestampDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONTimestamp
}

trait BSONUTCDateTimeSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONUTCDateTime
}

trait BSONUTCDateTimeDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONUTCDateTime
}

object DefaultBSONMarshaller {

  implicit object DefaultBSONBooleanDeser extends BSONBooleanDeserializer[Boolean] {
    def toNative(bsonType: BSONBoolean) = bsonType.booleanValue
  }

  implicit object DefaultBSONBooleanSer extends BSONBooleanSerializer[Boolean] {
    def toBSONType(native: Boolean) =
      if (native) BSONBooleanTrue else BSONBooleanFalse
  }

  implicit object DefaultBSONDoubleDeser extends BSONDoubleDeserializer[Double] {
    def toNative(bsonType: BSONDouble) = bsonType.primitiveValue
  }

  implicit object DefaultBSONDoubleSer extends BSONDoubleSerializer[Double] {
    def toBSONType(native: Double) = BSONDouble(native)
  }

  implicit object DefaultBSONIntegerDeser extends BSONIntegerDeserializer[Integer] {
    def toNative(bsonType: BSONInteger) = bsonType.primitiveValue
  }

  implicit object DefaultBSONIntegerSer extends BSONIntegerSerializer[Integer] {
    def toBSONType(native: Integer) = BSONInteger(native)
  }

  implicit object DefaultBSONLongDeser extends BSONLongDeserializer[Long] {
    def toNative(bsonType: BSONLong) = bsonType.primitiveValue
  }

  implicit object DefaultBSONLongSer extends BSONLongSerializer[Long] {
    def toBSONType(native: Long) = BSONLong(native)
  }

  implicit object DefaultBSONRegexDeser extends BSONRegexDeserializer[scala.util.matching.Regex] with BSONRegexFlags {
    /**
      * [Regular expression]
      *
      * The first cstring is the regex pattern,
      * the second is the regex options string.
      *
      * Options are identified by characters,
      * which must be stored in alphabetical order.
      *
      * Valid options are:
      * 'i' for case insensitive matching,
      * 'm' for multiline matching,
      * 'x' for verbose mode,
      * 'l' to make \w, \W, etc. locale dependent,
      * 's' for dotall mode ('.' matches everything),
      * 'u' to make \w, \W, etc. match unicode.
      */
    def toNative(bsonType: BSONRegex) = {
     "(?%s)%s".format(bsonType.flags, bsonType.pattern).r
    }
  }

  implicit object DefaultBSONRegexSer extends BSONRegexSerializer[scala.util.matching.Regex] with BSONRegexFlags {
    /**
      * [Regular expression]
      *
      * The first cstring is the regex pattern,
      * the second is the regex options string.
      *
      * Options are identified by characters,
      * which must be stored in alphabetical order.
      *
      * Valid options are:
      * 'i' for case insensitive matching,
      * 'm' for multiline matching,
      * 'x' for verbose mode,
      * 'l' to make \w, \W, etc. locale dependent,
      * 's' for dotall mode ('.' matches everything),
      * 'u' to make \w, \W, etc. match unicode.
      */
    def toBSONType(native: scala.util.matching.Regex) = {
      val buf = StringBuilder.newBuilder
      var _flags = native.pattern.flags
      for (flag <- Flags) {
        if ((_flags & flag.javaCode) > 0) {
          buf += flag.charCode
          _flags -= flag.javaCode
        }
      }

      assume(_flags == 0, "Some RegEx flags were not recognized.")

      val strFlags = buf.result()

      BSONRegex(native.pattern.pattern, strFlags)
    }
  }

  implicit object DefaultBSONStringDeser extends BSONStringDeserializer[String] {
    def toNative(bsonType: BSONString) = bsonType.primitiveValue
  }

  implicit object DefaultBSONStringSer extends BSONStringSerializer[String] {
    def toBSONType(native: String) = BSONString(native)
  }

  implicit object DefaultBSONSymbolDeser extends BSONSymbolDeserializer[Symbol] {
    def toNative(bsonType: BSONSymbol) = bsonType.primitiveValue
  }

  implicit object DefaultBSONSymbolSer extends BSONSymbolSerializer[Symbol] {
    def toBSONType(native: Symbol) = BSONSymbol(native.name)
  }

  implicit object DefaultBSONTimestampDeser extends BSONTimestampDeserializer[MongoTimestamp] {
    def toNative(bsonType: BSONTimestamp) =
      new MongoTimestamp(bsonType.primitiveValue)
  }

  implicit object DefaultBSONTimestampSer extends BSONTimestampSerializer[MongoTimestamp] {
    def toBSONType(native: MongoTimestamp) =
      new BSONTimestamp(native.increment, native.epoch)
  }

  implicit object DefaultBSONUTCDateTimeDeser extends BSONUTCDateTimeDeserializer[java.util.Date] {
    def toNative(bsonType: BSONUTCDateTime) =
      new java.util.Date(bsonType.primitiveValue)
  }

  implicit object DefaultBSONUTCDateTimeSer extends BSONUTCDateTimeSerializer[java.util.Date] {
    def toBSONType(native: java.util.Date) = BSONUTCDateTime(native.getTime)
  }
}

// vim: set ts=2 sw=2 sts=2 et:
