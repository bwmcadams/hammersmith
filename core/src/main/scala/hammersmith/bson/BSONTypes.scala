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


import hammersmith.bson.Logging 

import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.util.control.Exception.catching
import scala.util.matching.Regex


/*implicit def pimpByteString(str: ByteString): BSONByteString = 
  new BSONByteString(str)

sealed class BSONByteString {
  def take
}*/

class BSONParsingException(message: String, t: Throwable) extends Exception(message, t)


trait BSONType {
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
  def readCString(frame: ByteIterator, buffer: StringBuilder = new StringBuilder): String = {
    val c = frame.next()

    if (c == 0x00) {
      buffer.toString
    } else {
      buffer += c
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
    val data = frame.getBytes(buf)
    val parse = catching(classOf[UnsupportedOperationException]).withApply { e =>
      throw new BSONParsingException("Unable to decode UTF8 String from BSON.", e)
    } 
    parse { new String(data, 0, size - 1, "UTF-8") }
  }

  def readLong(bytes: Array[Byte], endianness: ByteOrder) = {
    var x = 0
    endianness match {
      case littleEndian => 
        x |= (0xFF & buf(0)) << 0
        x |= (0xFF & buf(1)) << 8
        x |= (0xFF & buf(2)) << 16
        x |= (0xFF & buf(3)) << 24
      case bigEndian => 
        x |= (0xFF & buf(0)) << 24 
        x |= (0xFF & buf(1)) << 16
        x |= (0xFF & buf(2)) << 8
        x |= (0xFF & buf(3)) << 0
    }
    x
  }

  def parseUUID(bytes: Array[Byte], endianness: ByteOrder = bigEndian) = {

    if (_binLen != 16) 
      throw new BSONParsingException("Invalid UUID Length in Binary. Expected 16, got " + _binLen)

    val mostSignificant = readLong(bytes, endianness)
    val leastSignificant = readLong(bytes, endianness)
    BSONBinaryUUIDContainer(mostSignificant, leastSignificant)
  }
}

/** BSON End of Object Marker - indicates a Doc / BSON Block has ended */
class BSONEndOfObject extends BSONType {
  val typeCode: Byte = 0x00	  

  def unapply(frame: ByteIterator): Boolean = 
    frame.head == typeCode 
 
}

/** BSON null value */
class BSONNull extends BSONType {
  val typeCode: Byte = 0x0A

  def unapply = noopUnapply

/** BSON Undefined value - deprecated in the BSON Spec; use null*/
class BSONUndef extends BSONType {
  val typeCode: Byte = 0x06 

  def unapply = noopUnapply
}

/** BSON Floating point Number */
class BSONDouble extends BSONType {
  val typeCode: Byte = 0x01

  def unapply(frame: ByteIterator): Option[(String, Double)] = 
    if (frame.head == typeCode) {
      Some((readCString(frame.drop(1), frame.getDouble))
    } else None
}

/** BSON String - UTF8 Encoded with length at beginning */
class BSONString extends BSONType {
  val typeCode: Byte = 0x02

  // TODO - Performant UTF8 parsing
  def unapply(frame: ByteIterator): Option[(String, String)] = 
    if (frame.head == type) {
      Some((readCString(frame.drop(1)), readUTF8String(frame)))
    } else None
}


trait BSONBinaryContainer 
case class BSONBinaryUUIDContainer(mostSignificant: Long, leastSignificant: Long) extends BSONBinaryContainer
case class BSONBinaryMD5Container(bytes: Array[Byte]) extends BSONBinaryContainer
case class BSONBinaryContainer(bytes: Array[Byte]) extends BSONBinaryContainer
case class BSONUserDefinedBinaryContainer(bytes: Array[Byte]) extends BSONBinaryContainer

/** 
 * BSON Binary - can actually be of several subtypes 
 *  
 * TODO - User customised container classes for subtypes 
 */
class BSONBinary extends BSONType with Logging {
  val typeCode: Byte = 0x05

  val Binary_Generic: Byte = 0x00
  val Binary_Function: Byte = 0x01
  val Binary_Old: Byte = 0x02
  val Binary_UUID_Old: Byte = 0x03
  val Binary_UUID: Byte = 0x04
  val Binary_MD5: Byte = 0x05
  val Binary_UserDefined: Byte = 0x80

  def unapply(frame: ByteIterator): Option[(String, BSONBinaryContainer)] = 
    if (frame.head == type) {
      val name = readCString(frame.drop(1))
      val _binLen = frame.getInt
      val _subType = frame.next()
      log.debug("/// PARSING BINARY of SubType '%s' and length '%s'", _subType, _binLen)

      val bin = new Array[Byte](_binLen)

      // TODO - Efficiency!
      if (_subType == Binary_Old)
        // Old binary format contained an extra length header 
        // parse out before passing 
        frame.drop(4) // drop the extra length header
      } 

      frame.getBytes(bin)

      _subType match {
        case Binary_UUID => 
          // "NEW" UUID Format uses big endianness, not little 
          Some(name, parseUUID(bin)) 
        case Binary_UUID_Old => 
          // "Old" UUID format used little endianness which is NOT how UUIDs are encoded
          Some(name, parseUUID(bin, littleEndian))
        case Binary_MD5 => 
          if (_binLen != 16) 
            throw new BSONParsingException("Invalid MD5 Length in Binary. Expected 16, got " + _binLen)
          // TODO - parse MD5
          Some(name, BSONBinaryMD5Container)
        case Binary | Binary_Old =>
          Some(name, BSONBinaryContainer(bin))
        case Binary_UserDefined | default => 
          Some(name, BSONUserDefinedBinaryContainer(bin))
      }
    } else None 
}

/** BSON ObjectID */
class BSONObjectID extends BSONType {
  val typecode: Byte = 0x07

  def unapply(frame: ByteIterator): Option[(String, ObjectID)] =
    if (frame.head == type) {
      // Because MongoDB Loves consistency, OIDs are stored Big Endian
      val name = readCString(frame.drop(1))
      val timestamp = frame.getInt(bigEndian)
      val machineID = frame.getInt(bigEndian)
      val increment = frame.getInt(bigEndian)
      Some(name, ObjectID(timestamp, machineID, increment, false))
    } else None
}

/** BSON Boolean */
class BSONBoolean extends BSONType {
  val typeCode: Byte = 0x08

  def unapply(frame: ByteIterator): Option[(String, Boolean)] =
    if (frame.head == type) {
      val name = readCString(frame.drop(1))
      val bool = frame.next() == 0x01
      Some(name, bool) 
    } else None
}

/** BSON Timestamp, UTC  - because MongoDB still doesn't support timezones */
class BSONUTCDateTime extends BSONType {
  val typeCode: Byte = 0x09

  /**
   * Because different users will have different desired types
   * for their dates, return a LONG representing epoch seconds 
   */
  def unapply(frame: ByteIterator): Option[(String, Long)] = 
    if (frame.head == type) {
      Some(readCString(frame.drop(1)), frame.getLong)
    } else None
}

//case class BSONRegExHolder(val name: String, val pattern: String, val options: String)

/** BSON Regular Expression */
class BSONRegEx extends BSONType {
  val typeCode: Byte = 0x10

  /** TODO - Use a placeholder object that can be converted to the users 
    * desired regex representation? */
  def unapply(frame: ByteIterator): Option[String, Regex] = 
    if (frame.head == type ) {
      val name = readCString(frame.drop(1))
      val pattern = readCString(frame)
      val options = readCString(frame)
      Some(name, "(?%s)%s".format(options, pattern).r)
    } else None

}

// Currently no dereferencing support, etc. (not a fan anyway)
case class DBRef(namespace: String, oid: ObjectID) 

/** BSON DBRefs */
class BSONDBref extends BSONType {
  val typeCode: Byte = 0x0C

  def unapply(frame: ByteIterator): Option[String, Regex] = 
    if (frame.head == type) {
      val name = readCString(frame.drop(1))
      val namespace = readCString(frame)
      /*
       * again with consistency.
       * though not documented anywhere, while normal OIDs are stored 
       * big endian, in the java driver code the dbref ones are little. WTF?
       */
      val timestamp = frame.getInt()
      val machineID = frame.getInt()
      val increment = frame.getInt()
      Some(name, DBRef(namespace, ObjectID(timestamp, machineID, increment, false)))
    } else None

}

case class BSONCode(code: String)

/** BSON JS Code ... basically a block of javascript stored in DB */
class BSONJSCode extends BSONType {
  val typeCode: Byte = 0x0D


  def unapply(frame: ByteIterator) = ???
}

// needs a documnt for scope
case class BSONCodeWScope(code: String, scope: ???)

/** BSON Document */
class BSONDocument extends BSONType {
  val typeCode: Byte = 0x03

  def unapply(frame: ByteIterator) = ???
}

/** BSON Array */
class BSONArray extends BSONType {
  val typeCode: Byte = 0x04

  def unapply(frame: ByteIterator) = ???
}
