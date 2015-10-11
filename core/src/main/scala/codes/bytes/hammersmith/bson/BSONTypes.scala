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
package codes.bytes.hammersmith.bson


import java.nio.ByteOrder
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.util.control.Exception.catching
import scala.util.matching.Regex
import scala.collection.immutable.Queue
import akka.util.ByteIterator
import codes.bytes.hammersmith.collection.immutable.{Document, DBList}
import scala.util.control.Exception._
import java.util.NoSuchElementException
import scala.NoSuchElementException


sealed trait BSONType extends StrictLogging {
  def typeCode: Byte

  val littleEndian = ByteOrder.LITTLE_ENDIAN
  val bigEndian = ByteOrder.BIG_ENDIAN

  /** All ops default to little endianness, which is the encoding of BSON */
  implicit val byteOrder = littleEndian

  /** 
   * A "NOOP" Unapply for certain field types that are *just* a field name
   */
  def noopUnapply(frame: ByteIterator): Option[String] = 
    if (frame.head == typeCode) {
      Some(readCString(frame.drop(1))) // drop the header type byte
    } else None

  /**
   * Read a BSON CString.
   * 
   * BSON CStrings are an example of bad network frame design:
   * Rather than containing a length header, they are
   * Byte* terminated by \x00
   */
  @tailrec
  final def readCString(frame: ByteIterator, buffer: StringBuilder = new StringBuilder): String = {

    val c = frame.next().toChar

    if (c == 0x00) {
      buffer.toString
    } else {
      buffer.append(c)
      readCString(frame, buffer)
    }
  }

  /**
   * Read a BSON UTF8 String (0x02)
   * Length(int32) followed by $Length bytes and an additional \x00 for obnoxiousness' sake
   * TODO - NONPERFORMANT! Use UTF8 parsing routines from Jackson or Postgres driver!
   */
  def readUTF8String(frame: ByteIterator): String = {
    val size = frame.getInt
    logger.trace(s"Attempting to read a UTF8 string of '$size' bytes.")
    require(size < BSONDocumentType.MaxSize,
      "Invalid UTF8 String. Expected size less than Max BSON Size of '%s'. Got '%s'".format(BSONDocumentType.MaxSize, size))
    val buf = new Array[Byte](size)
    frame.getBytes(buf)
    // TODO - Catching is heavy on the profiler.. lets try to leave it out for now (though may hav runtime blowup :/)
    val parse = catching(classOf[UnsupportedOperationException]).withApply { e =>
      throw new BSONParsingException("Unable to decode UTF8 String from BSON.", e)
    } 
    parse {
      new String(buf, 0, size - 1, "UTF-8")
    }
  }

  def readInt(bytes: Array[Byte])(implicit endianness: ByteOrder) = {
    var x = 0
    endianness match {
      case `littleEndian` => 
        x |= (0xFF & bytes(0)) << 0
        x |= (0xFF & bytes(1)) << 8
        x |= (0xFF & bytes(2)) << 16
        x |= (0xFF & bytes(3)) << 24
      case `bigEndian` => 
        x |= (0xFF & bytes(0)) << 24
        x |= (0xFF & bytes(1)) << 16
        x |= (0xFF & bytes(2)) << 8
        x |= (0xFF & bytes(3)) << 0
    }
    x
  }

  def readLong(bytes: Array[Byte])(implicit endianness: ByteOrder) = {
    var x = 0L
    endianness match {
      case `littleEndian` =>
        x |= (0xFFL & bytes(0)) << 0
        x |= (0xFFL & bytes(1)) << 8
        x |= (0xFFL & bytes(2)) << 16
        x |= (0xFFL & bytes(3)) << 24
        x |= (0xFFL & bytes(4)) << 32
        x |= (0xFFL & bytes(5)) << 40
        x |= (0xFFL & bytes(6)) << 48
        x |= (0xFFL & bytes(7)) << 56
      case `bigEndian` =>
        x |= (0xFF & bytes(0)) << 56
        x |= (0xFF & bytes(1)) << 48
        x |= (0xFF & bytes(2)) << 40
        x |= (0xFF & bytes(3)) << 32
        x |= (0xFF & bytes(4)) << 24
        x |= (0xFF & bytes(5)) << 16
        x |= (0xFF & bytes(6)) << 8
        x |= (0xFF & bytes(7)) << 0
    }
    x
  }

  def parseUUID(bytes: Array[Byte], endianness: ByteOrder = bigEndian) = {
    val _binLen = bytes.length

    if (_binLen != 16)
      throw new BSONParsingException("Invalid UUID Length in Binary. Expected 16, got " + _binLen)

    val mostSignificant = readLong(bytes.slice(0, 8))(endianness)
    val leastSignificant = readLong(bytes.slice(8, 16))(endianness)
    BSONBinaryUUID(mostSignificant, leastSignificant)
  }
}

/** BSON End of Object Marker - indicates a Doc / BSON Block has ended */
object BSONEndOfObjectType extends BSONType {
  val typeCode: Byte = 0x00

  def unapply(frame: ByteIterator): Option[Boolean] =
    if (frame.head == typeCode) Some(true) else None


}

sealed trait SpecialBSONValue
/** BSON Min Key and Max Key represent special internal types for Sharding */
case object BSONMinKey extends SpecialBSONValue
case object BSONMaxKey extends SpecialBSONValue
/** The dumbest types I've ever seen on earth */
case object BSONNull extends SpecialBSONValue
case object BSONUndef extends SpecialBSONValue

/** BSON null value */
object BSONNullType extends BSONType {
  val typeCode: Byte = 0x0A

  def unapply(frame: ByteIterator) = noopUnapply(frame)
}

/** BSON Undefined value - deprecated in the BSON Spec; use null*/
object BSONUndefType extends BSONType {
  val typeCode: Byte = 0x06

  def unapply(frame: ByteIterator) = noopUnapply(frame)
}

/** BSON Floating point Number */
object BSONDoubleType extends BSONType {
  val typeCode: Byte = 0x01

  def unapply(frame: ByteIterator): Option[(String, Double)] =
    if (frame.head == typeCode) {
      Some((readCString(frame.drop(1)), frame.getDouble))
    } else None
}

/** BSON String - UTF8 Encoded with length at beginning */
object BSONStringType extends BSONType {
  val typeCode: Byte = 0x02

  // TODO - Performant UTF8 parsing
  def unapply(frame: ByteIterator): Option[(String, String)] = 
    if (frame.head == typeCode) {
      Some((readCString(frame.drop(1)), readUTF8String(frame)))
    } else None
}


trait BSONBinaryContainer
case class BSONBinaryUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinaryContainer
case class BSONBinaryMD5(bytes: Array[Byte]) extends BSONBinaryContainer
case class BSONBinary(bytes: Array[Byte]) extends BSONBinaryContainer
case class BSONBinaryUserDefined(bytes: Array[Byte]) extends BSONBinaryContainer

/** 
 * BSON Binary - can actually be of several subtypes 
 *  
 * TODO - User customised container classes for subtypes 
 */
object BSONBinaryType extends BSONType with StrictLogging {
  val typeCode: Byte = 0x05

  val Binary_Generic: Byte = 0x00
  val Binary_Function: Byte = 0x01
  val Binary_Old: Byte = 0x02
  val Binary_UUID_Old: Byte = 0x03
  val Binary_UUID: Byte = 0x04
  val Binary_MD5: Byte = 0x05
  val Binary_UserDefined: Byte = 0x80.toByte

  def unapply(frame: ByteIterator): Option[(String, BSONBinaryContainer)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val _binLen = frame.getInt
      val _subType = frame.next()
      logger.debug(s"/// PARSING BINARY of SubType '${_subType}' and length '${_binLen}'")

      val bin = new Array[Byte](_binLen)

      // TODO - Efficiency!
      if (_subType == Binary_Old) {
        // Old binary format contained an extra length header 
        // parse out before passing 
        frame.drop(4) // drop the extra length header
      } 

      frame.getBytes(bin)

      _subType match {
        case Binary_UUID =>
          // "NEW" UUID Format uses big endianness, not little 
          Some((name, parseUUID(bin)))
        case Binary_UUID_Old =>
          // "Old" UUID format used little endianness which is NOT how UUIDs are encoded
          Some((name, parseUUID(bin, littleEndian)))
        case Binary_MD5 =>
          if (_binLen != 16) 
            throw new BSONParsingException("Invalid MD5 Length in Binary. Expected 16, got " + _binLen)
          // TODO - parse MD5
          Some((name, BSONBinaryMD5(bin)))
        case Binary_Generic | Binary_Old =>
          Some((name, BSONBinary(bin)))
        case Binary_UserDefined =>
          Some((name, BSONBinaryUserDefined(bin)))
        case other => 
          Some((name, BSONBinaryUserDefined(bin)))
      }
    } else None 
}

/** BSON ObjectID */
object BSONObjectIDType extends BSONType {
  val typeCode: Byte = 0x07

  def unapply(frame: ByteIterator): Option[(String, ObjectID)] =
    if (frame.head == typeCode) {
      // Because MongoDB Loves consistency, OIDs are stored Big Endian
      val name = readCString(frame.drop(1))
      val timestamp = frame.getInt(bigEndian)
      val machineID = frame.getInt(bigEndian)
      val increment = frame.getInt(bigEndian)
      val oid = ObjectID(timestamp, machineID, increment, false)
      logger.trace(s"Parsed out an ObjectID in '$name' from BSON '$oid'")
      Some(name, oid)
    } else None
}

/** BSON Boolean */
object BSONBooleanType extends BSONType {
  val typeCode: Byte = 0x08

  def unapply(frame: ByteIterator): Option[(String, Boolean)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val bool = frame.next() == 0x01
      Some(name, bool) 
    } else None
}

/** BSON Timestamp, UTC  - because MongoDB still doesn't support timezones */
object BSONDateTimeType extends BSONType {
  val typeCode: Byte = 0x09

  /**
   * Because different users will have different desired types
   * for their dates, return a LONG representing epoch seconds 
   */
  def unapply(frame: ByteIterator): Option[(String, Long)] = 
    if (frame.head == typeCode) {
      Some(readCString(frame.drop(1)), frame.getLong)
    } else None
}


/** BSON Regular Expression */
object BSONRegExType extends BSONType {
  val typeCode: Byte = 0x0B

  import java.util.regex.Pattern

  case class Flag(javaCode: Int, charCode: Char)
  val CanonEq = Flag(Pattern.CANON_EQ, 'c')
  val UnixLines = Flag(Pattern.UNIX_LINES, 'd')
  val Global = Flag(256, 'g')
  val CaseInsensitive = Flag(Pattern.CASE_INSENSITIVE, 'i')
  val Multiline = Flag(Pattern.MULTILINE, 'm')
  val DotAll = Flag(Pattern.DOTALL, 's')
  val Literal = Flag(Pattern.LITERAL, 't')
  val UnicodeCase = Flag(Pattern.UNICODE_CASE, 'u')
  val Comments = Flag(Pattern.COMMENTS, 'x')
  val Flags = List(CanonEq, UnixLines, Global, CaseInsensitive, Multiline, DotAll, Literal, UnicodeCase, Comments)

  def unapply(frame: ByteIterator): Option[(String, Regex)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val pattern = readCString(frame)
      val options = readCString(frame)
      Some(name, "(?%s)%s".format(options, pattern).r)
    } else None

  def parseFlags(flags: Int) = {
    val buf = StringBuilder.newBuilder
    var _flags = flags
    for (flag <- Flags) {
      if ((_flags & flag.javaCode) > 0) {
        buf += flag.charCode
        _flags -= flag.javaCode
      }
    }

    assume(_flags == 0, "Some RegEx flags were not recognized.")

    buf.result()
  }

}

// Currently no dereferencing support, etc. (not a fan anyway)
final case class DBRef(namespace: String, oid: ObjectID)

/** BSON DBPointers are deprecated, in favor of a 'parsed aware' DBRef which is stupid and slows parsers down. */
object BSONDBPointerType extends BSONType {
  val typeCode: Byte = 0x0C

  def unapply(frame: ByteIterator): Option[(String, DBRef)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val namespace = readCString(frame)
      /*
       * again with consistency.
       * though not documented anywhere, while normal OIDs are stored 
       * big endian, in the java driver code the dbref ones are little. WTF?
       */
      val timestamp = frame.getInt
      val machineID = frame.getInt
      val increment = frame.getInt
      Some((name, DBRef(namespace, ObjectID(timestamp, machineID, increment, false))))
    } else None

}

final case class BSONCode(code: String)

/** BSON JS Code ... basically a block of javascript stored in DB */
object BSONJSCodeType extends BSONType {
  val typeCode: Byte = 0x0D


  def unapply(frame: ByteIterator): Option[(String, BSONCode)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val code = readUTF8String(frame)
      logger.debug(s"JSCode at '$name' - '$code'")
      Some((name, BSONCode(code)))
    } else None
}

/** BSON Symbol - not used a lot, but for languages that support symbol */
object BSONSymbolType extends BSONType {
  val typeCode: Byte = 0x0E

  def unapply(frame: ByteIterator): Option[(String, Symbol)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      logger.trace(s"Symbol Name: '$name'  / ${frame.len}")
      val sym = readUTF8String(frame)
      logger.trace(s"Symbol Value: '$name'")
      Some((name, Symbol(sym)))
    } else None
}

/** BSON 32 Bit Integer */
object BSONInt32Type extends BSONType {
  val typeCode: Byte = 0x10

  def unapply(frame: ByteIterator): Option[(String, Int)] = 
    if (frame.head == typeCode) {
      // The FIELD name, hence cString
      val name = readCString(frame.drop(1))
      Some((name, frame.getInt))
    } else None
}

/** BSON 64 Bit Integer  - aka a long */
object BSONInt64Type extends BSONType {
  val typeCode: Byte = 0x12

  def unapply(frame: ByteIterator): Option[(String, Long)] =
    if (frame.head == typeCode) {
      val rest = frame.drop(1)
      // The FIELD name, hence cString
      val name = readCString(rest)
      Some((name, frame.getLong))
    } else None
}

final case class BSONTimestamp(time: Int, increment: Int)

/** BSON Timestamp - this is a special type for sharding, oplog etc */
object BSONTimestampType extends BSONType {
  val typeCode: Byte = 0x11

  def unapply(frame: ByteIterator): Option[(String, BSONTimestamp)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONTimestamp(increment=frame.getInt, time=frame.getInt)))
    } else None
}


object BSONMinKeyType extends BSONType {
  val typeCode: Byte = 0xFF.toByte

  def unapply(frame: ByteIterator): Option[(String, BSONMinKey.type)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONMinKey))
    } else None
}

object BSONMaxKeyType extends BSONType {
  val typeCode: Byte = 0x7F

  def unapply(frame: ByteIterator): Option[(String, BSONMaxKey.type)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONMaxKey))
    } else None
}

// needs a document for scope
final case class BSONCodeWScope(code: String, scope: Map[String, Any])

/** BSON JS Code with a scope ... basically a block of javascript stored in DB */
object BSONScopedJSCodeType extends BSONType {
  val typeCode: Byte = 0x0F

  def unapply(frame: ByteIterator)(implicit childParser: BSONParser[_]): Option[(String, BSONCodeWScope)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val size = frame.getInt
      val code = readUTF8String(frame)
      // TODO - READ SCOPE
      logger.trace(s"JSCode at '$name' - '$code'")
      val scopeSize = frame.getInt
      val scope = Map[String, Any](childParser.parse(frame): _*)
      logger.trace(s"Scope: $scope")
      Some((name, BSONCodeWScope(code, scope)))
    } else None
}

/** BSON Document 
  * return a list of key/value pairs
  */
object BSONDocumentType extends BSONType {
  val typeCode: Byte = 0x03
  val MaxSize = 16 * 1024 * 1024 // 16 MB

  def unapply(frame: ByteIterator)(implicit childParser: BSONParser[_]): Option[(String, Seq[(String, Any)])] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val subLen = frame.getInt - 4
      require(subLen < BSONDocumentType.MaxSize,
        "Invalid embedded document. Expected size less than Max BSON Size of '%s'. Got '%s'".format(BSONDocumentType.MaxSize, subLen))
      val bytes = frame.clone().take(subLen)
      frame.drop(subLen)
      logger.trace(s"Reading an embedded BSON object of '$subLen' bytes.")
      val doc = childParser.parse(bytes)
      logger.trace(s"*** ${frame.head} *** Parsed a set of subdocument entries for '$name': '$doc'")
      Some((name, doc))
    } else None
}

/** BSON Array 
  * return a list of entries */
object BSONArrayType extends BSONType {
  val typeCode: Byte = 0x04

  def unapply(frame: ByteIterator)(implicit childParser: BSONParser[_]): Option[(String, Seq[Any])] =
   if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val subLen = frame.getInt - 4
      require(subLen < BSONDocumentType.MaxSize,
       "Invalid embedded array. Expected size less than Max BSON Size of '%s'. Got '%s'".format(BSONDocumentType.MaxSize, subLen))
      val bytes = frame.clone().take(subLen)
      frame.drop(subLen)
      logger.trace(s"Reading a BSON Array of '$subLen' bytes.")
      val doc = childParser.parse(bytes)
      logger.trace(s"Parsed a set of subdocument entries for '$name': '$doc'")
      /*
       * I have seen no contractual guarantees that the array items are in order
       * in mongo, but most drivers assume it, so shall we...
       * Flatten out and ignore the keys
       */ 
      Some((name, doc.map(_._2)))
    } else None
}
