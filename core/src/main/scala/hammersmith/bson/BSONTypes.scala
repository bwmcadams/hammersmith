/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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
package hammersmith.bson


import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.util.control.Exception.catching
import scala.util.matching.Regex
import scala.collection.immutable.Queue
import akka.util.ByteIterator
import hammersmith.collection.immutable.{Document, DBList}
import hammersmith.util.Logging


/*implicit def pimpByteString(str: ByteString): BSONByteString =
  new BSONByteString(str)

sealed class BSONByteString {
  def take
}*/

class BSONParsingException(message: String, t: Throwable = null) extends Exception(message, t)

/** T is your toplevel document type. */
trait BSONParser[T] extends Logging {
  implicit val thisParser = this

  private def hexDump(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString

  /** The "core" parse routine; should break down your BSON into T */
  def unapply(frame: ByteIterator): T = {
    // Extract the BSON doc
    val len = frame.getInt(ByteOrder.LITTLE_ENDIAN)
    val sz = frame.len
    log.debug(s"Frame Size: $sz Doc Size: $len")
    /* for the life of me i can't remember why i'm cloning
     * but i suspect it's bad as it doesn't let us move forward on the stream.
     * Clone outside here for stuff.
     */
    val data = frame.clone().take(len - 4)
    frame.drop(len - 4) // advance the iterator for other blocks
    log.debug(s"Parsing a BSON doc of $len bytes, with a data block of " + data.len)
    parseRootObject(parse(data))
  }

  /** Parses a sequence of entries into a Root object, which must be of type T
   * Separated from parseDocument to allow for discreet subdocument types (which may backfire on me)
   */
  def parseRootObject(entries: Seq[(String, Any)]): T 

  /** Parse documents... */
  @tailrec
  protected[bson] final def parse(frame: ByteIterator, entries: Queue[(String, Any)] = Queue.empty[(String, Any)]): Queue[(String, Any)] = {
    val typ = frame.head
    val _entries: Queue[(String, Any)] = frame match {
      case BSONEndOfObjectType(eoo) =>
        log.trace("Got a BSON End of Object")
        entries
      case BSONNullType(field) =>
        // TODO - how best to represent nulls / undefs?
        log.trace("Got a BSON Null for field '%s'", field)
        entries :+ (field, null)
      case BSONUndefType(field) =>
        // TODO - how best to represent nulls / undefs?
        log.trace("Got a BSON Null for field '%s'", field)
        entries :+ (field, null)
      case BSONDoubleType(field, value) =>
        log.trace("Got a BSON Double '%s' for field '%s'", value, field)
        entries :+ (field, parseDouble(field, value))
      case BSONStringType(field, value) =>
        log.trace("Got a BSON String '%s' for field '%s'", value, field)
        entries :+ (field, parseString(field, value))
      case BSONDocumentType(field, values) =>
        log.trace("Got a BSON entries '%s' for field '%s'", values, field)
        entries :+ (field, parseDocument(field, values))
      case BSONArrayType(field, value) =>
        log.trace("Got a BSON Array '%s' for field '%s'", value, field)
        entries :+ (field, parseArray(field, value))
      case BSONBinaryType(field, value) =>
        log.trace("Got a BSON Binary for field '%s'", field)
        entries :+ (field, parseBinary(field, value))
      case BSONObjectIDType(field, value) =>
        log.trace("Got a BSON ObjectID for field '%s'", field)
        entries :+ (field, parseObjectID(field, value))
      case BSONBooleanType(field, value) =>
        log.trace("Got a BSON Boolean '%s' for field '%s'", value, field)
        // Until someone proves otherwise to me, don't see a reason for custom Bool
        entries :+ (field, value)
      case BSONUTCDateTimeType(field, value) =>
        log.trace("Got a BSON UTC Timestamp '%s' for field '%s'", value, field)
        entries :+ (field, parseDateTime(field, value))
      case BSONRegExType(field, value) =>
        log.trace("Got a BSON Regex '%s' for field '%s'", value, field)
        entries :+ (field, parseRegEx(field, value))
      case BSONDBRefType(field, value) =>
        log.trace("Got a BSON DBRef '%s' for field '%s'", value, field)
        // no custom parsing for dbRef until necessity is proven
        entries :+ (field, value)
      case BSONJSCodeType(field, value) =>
        log.trace("Got a BSON JSCode for field '%s'", field)
        // no custom parsing for now
        entries :+ (field, value)
      case BSONJSCodeWScopeType(field, value) =>
        log.trace("Got a BSON JSCode W/ Scope for field '%s'", field)
        // no custom parsing for now
        entries :+ (field, value)
      case BSONSymbolType(field, value) =>
        log.trace("Got a BSON Symbol '%s' for field '%s'", value, field)
        entries :+ (field, parseSymbol(field, value))
      case BSONInt32Type(field, value) =>
        log.trace("Got a BSON Int32 '%s' for field '%s'", value, field)
        entries :+ (field, parseInt32(field, value))
      case BSONInt64Type(field, value) =>
        log.trace("Got a BSON Int64 (Long) '%s' for field '%s'", value, field)
        entries :+ (field, parseInt64(field, value))
      case BSONTimestampType(field, value) =>
        log.trace("Got a BSON Timestamp '%s' for field '%s'", value, field)
        // because of it's use as an internal type, not allowing custom for now
        entries :+ (field, value)
      case BSONMinKeyType(field, value) =>
        log.trace("Got a BSON MinKey for field '%s'", field)
        // because of it's use as an internal type, not allowing custom
        entries :+ (field, value)
      case BSONMaxKeyType(field, value) =>
        log.trace("Got a BSON MaxKey for field '%s'", field)
        // because of it's use as an internal type, not allowing custom
        entries :+ (field, value)
      case unknown =>
        log.warning(s"Unknown or unsupported BSON Type '$typ' / $unknown")
        throw new BSONParsingException(s"No support for decoding BSON Type of byte '$typ'/$unknown")
      }
    if (BSONEndOfObjectType.typeCode == typ) {
      log.trace("***** EOO")
      frame.next()
      _entries
    } else parse(frame, _entries)
  }

  /** 
   * Overridable method for how to handle adding a int32 entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseInt32(field: String, value: Double): Any = value

 /** 
   * Overridable method for how to handle adding a int64 entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseInt64(field: String, value: Double): Any = value

 /** 
   * Overridable method for how to handle adding a document entry
   * You should compose the lists of Key/Value pairs into a doc of some kind
   *
   * Defaults to Document from the builtin hammersmith collection library.
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseDocument(field: String, values: Seq[(String, Any)]): Any = Document(values: _*)

 /** 
   * Overridable method for how to handle adding a array entry
   * You should compose the list of entries into a List structure of some kind
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseArray(field: String, values: Seq[Any]): Any = DBList(values: _*)

 /** 
   * Overridable method for how to handle adding a symbol entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseSymbol(field: String, value: Symbol): Any = value

 /** 
   * Overridable method for how to handle adding a regex entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseRegEx(field: String, value: Regex): Any = value

 /** 
   * Overridable method for how to handle adding a UTC Datetime entry
   * DateTime will be provided as a long representing seconds since 
   * Jan 1, 1970 
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseDateTime(field: String, value: Long): Any = new java.util.Date(value) 

  /** 
   * Overridable method for how to handle adding a double entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseDouble(field: String, value: Double): Any = value

  /** 
   * Overridable method for how to handle adding a string entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseString(field: String, value: String): Any = value

  /** 
   * Overridable method for how to handle adding an ObjectID entry
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseObjectID(field: String, value: ObjectID): Any = value

  /** 
   * Overridable method for how to handle adding a Binary entry
   * Remember there are several subcontainer types and you likely
   * want a pattern matcher for custom implementation
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseBinary(field: String, value: BSONBinaryContainer): Any = value match {
    case BSONBinaryUUID(most, least) => new java.util.UUID(most, least)
    //case BSONBinaryMD5(bytes)
    case other => value
  }
}

object DefaultBSONParser extends BSONParser[Document] {
  def parseRootObject(entries: Seq[(String, Any)]) = Document(entries: _*)
}

trait BSONType extends Logging {
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

  def readInt(bytes: Array[Byte], endianness: ByteOrder) = {
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

  def readLong(bytes: Array[Byte], endianness: ByteOrder) = {
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

    val mostSignificant = readLong(bytes.slice(0, 8), endianness)
    val leastSignificant = readLong(bytes.slice(8, 16), endianness)
    BSONBinaryUUID(mostSignificant, leastSignificant)
  }
}

/** BSON End of Object Marker - indicates a Doc / BSON Block has ended */
object BSONEndOfObjectType extends BSONType {
  val typeCode: Byte = 0x00

  def unapply(frame: ByteIterator): Option[Boolean] =
    if (frame.head == typeCode) Some(true) else None


}

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
object BSONBinaryType extends BSONType with Logging {
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
      log.debug("/// PARSING BINARY of SubType '%s' and length '%s'", _subType, _binLen)

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
      log.trace(s"Parsed out an ObjectID in '$name' from BSON '$oid'")
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
object BSONUTCDateTimeType extends BSONType {
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

  def unapply(frame: ByteIterator): Option[(String, Regex)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val pattern = readCString(frame)
      val options = readCString(frame)
      Some(name, "(?%s)%s".format(options, pattern).r)
    } else None

}

// Currently no dereferencing support, etc. (not a fan anyway)
case class DBRef(namespace: String, oid: ObjectID) 

/** BSON DBRefs */
object BSONDBRefType extends BSONType {
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

case class BSONCode(code: String)

/** BSON JS Code ... basically a block of javascript stored in DB */
object BSONJSCodeType extends BSONType {
  val typeCode: Byte = 0x0D


  def unapply(frame: ByteIterator): Option[(String, BSONCode)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val code = readUTF8String(frame)
      log.debug("JSCode at '%s' - '%s'", name, code)
      Some((name, BSONCode(code)))
    } else None
}

/** BSON Symbol - not used a lot, but for languages that support symbol */
object BSONSymbolType extends BSONType {
  val typeCode: Byte = 0x0E

  def unapply(frame: ByteIterator): Option[(String, Symbol)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val sym = readUTF8String(frame)
      Some((name, Symbol(sym)))
    } else None
}

/** BSON 32 Bit Integer */
object BSONInt32Type extends BSONType {
  val typeCode: Byte = 0x10

  def unapply(frame: ByteIterator): Option[(String, Int)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, frame.getInt))
    } else None
}

/** BSON 64 Bit Integer  - aka a long */
object BSONInt64Type extends BSONType {
  val typeCode: Byte = 0x12

  def unapply(frame: ByteIterator): Option[(String, Long)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, frame.getLong))
    } else None
}

case class BSONTimestamp(time: Int, increment: Int)

/** BSON Timestamp - this is a special type for sharding, oplog etc */
object BSONTimestampType extends BSONType {
  val typeCode: Byte = 0x11

  def unapply(frame: ByteIterator): Option[(String, BSONTimestamp)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONTimestamp(increment=frame.getInt, time=frame.getInt)))
    } else None
}

/** BSON Min Key and Max Key represent special internal types for Sharding */
case class BSONMinKey
case class BSONMaxKey 

object BSONMinKeyType extends BSONType {
  val typeCode: Byte = 0xFF.toByte

  def unapply(frame: ByteIterator): Option[(String, BSONMinKey)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONMinKey()))
    } else None
}

object BSONMaxKeyType extends BSONType {
  val typeCode: Byte = 0x7F

  def unapply(frame: ByteIterator): Option[(String, BSONMaxKey)] = 
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      Some((name, BSONMaxKey()))
    } else None
}

// needs a document for scope
case class BSONCodeWScope(code: String, scope: Map[String, Any])

/** BSON JS Code with a scope ... basically a block of javascript stored in DB */
object BSONJSCodeWScopeType extends BSONType {
  val typeCode: Byte = 0x0F

  def unapply(frame: ByteIterator)(implicit childParser: BSONParser[_]): Option[(String, BSONCodeWScope)] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val size = frame.getInt
      val code = readUTF8String(frame)
      // TODO - READ SCOPE
      log.trace("JSCode at '%s' - '%s'", name, code)
      val scopeSize = frame.getInt
      val scope = Map[String, Any](childParser.parse(frame): _*)
      log.trace("Scope: " + scope)
      Some((name, BSONCodeWScope(code, scope)))
    } else None
}

/** BSON Document 
  * return a list of key/value pairs
  */
object BSONDocumentType extends BSONType {
  val typeCode: Byte = 0x03

  def unapply(frame: ByteIterator)(implicit childParser: BSONParser[_]): Option[(String, Seq[(String, Any)])] =
    if (frame.head == typeCode) {
      val name = readCString(frame.drop(1))
      val subLen = frame.getInt - 4
      val bytes = frame.clone().take(subLen)
      frame.drop(subLen)
      log.trace("Reading an embedded BSON object of '%s' bytes.", subLen)
      val doc = childParser.parse(bytes)
      log.trace("*** %s *** Parsed a set of subdocument entries for '%s': '%s'", frame.head, name, doc)
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
      val bytes = frame.clone().take(subLen)
      frame.drop(subLen)
      log.trace("Reading a BSON Array of '%s' bytes.", subLen)
      val doc = childParser.parse(bytes)
      log.trace("Parsed a set of subdocument entries for '%s': '%s'", name, doc)
      /*
       * I have seen no contractual guarantees that the array items are in order
       * in mongo, but most drivers assume it, so shall we...
       * Flatten out and ignore the keys
       */ 
      Some((name, doc.map(_._2)))
    } else None
}
