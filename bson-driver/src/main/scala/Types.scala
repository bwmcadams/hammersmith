/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
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
package org.bson.scala
package types 

import scala.util.control.Exception._

import java.io.{IOException, InputStream}

import java.lang.{Double => JDouble}

trait BSONType[T] extends util.Logging {
  /* Type indicator byte */
  val tByte: Byte

  def unapply(t_byte: Int)(implicit stream: InputStream): Option[(String, T)] = 
    if (t_byte.byteValue != tByte) 
      None 
    else 
      catching(classOf[IOException]) opt (readCString, extract)
    
  def readInt(implicit stream: InputStream) = {
    var x = 0
    for (i <- 0 until 4) x |= ( 0xFF & stream.read ) << i * 8
    x
  }

  def readLong(implicit stream: InputStream) = {
    var x = 0L
    for (i <- 0 until 8) x |= ( 0xFFL & stream.read ) << i * 8
    x
  }

  def readDouble(implicit stream: InputStream) = JDouble.longBitsToDouble(readLong)

  def readCString(implicit stream: InputStream) = {
    var done = false
    val b = new StringBuilder
    do {
      stream.read match {
        case BSONTerminator.tByte => {
          done = true
        }
        case x => b.append(x.byteValue)
      }
    } while (!done)
    val str = new String(scala.io.Codec.fromUTF8(b.result))
    log.debug("CString: %s", str)
    str
  }


  def extract: T

}

object BSONTerminator extends BSONType[Any] {
  val tByte = 0x00

  def extract = Nil

}

object BSONDouble extends BSONType[Double] {
  val tByte = 0x01
  
  def extract(implicit stream: InputStream) = readDouble
}

object BSONString extends BSONType[String] {
  val tByte = 0x02

  def extract(implicit stream: InputStream) = {}
}

object BSONEmbeddedDocument extends BSONType[Map[String, _]] {
  val tByte = 0x03

  def extract(implicit stream: InputStream) = {}
}

object BSONArray extends BSONType[Array[_]] {
  val tByte = 0x04

  def extract(implicit stream: InputStream) = {}

}

trait BSONBinaryBase extends BSONType[Array[Byte]] {
  val tByte = 0x05
  
  object SubType extends Enumeration {
    val GenericBinary = 0x00
    val Function = 0x01
    val OldBinary = 0x02 // Deprecated format
    val UUID = 0x03
    val MD5 = 0x04
    val UserDefined = 0x80
  }
  
  def extract(implicit stream: InputStream): BSONBinaryData = {
    // TODO - UserDefined data support?
    val totalLen = readInt
    stream.read match {
      case SubType.GenericBinary => {
        var buf = Array.ofDim[Byte](totalLen)
        stream.read(buf)
        BSONGenericBinaryData(buf)
      }
      case SubType.OldBinary => {
        val len = readInt
        if (len + 4 != totalLen) 
          throw new InvalidBSONException("Error in Old Binary (0x02) subtype data; invalid data length.  { data length: %d / total length: %d }".format(len, totalLen))
        var buf = Array.ofDim[Byte](len)
        stream.read(buf)
        BSONOldBinaryData(buf)
      }
      // TODO - should we quietly handle valid subtypes and return generic array?
      case unsupported => 
        throw new InvalidBSONException("Unknown or Unsupported BSONBinary subtype: '%s'".format(unsupported))
    }
  }
}

object BSONBinary extends BSONBinaryBase

// TODO - Implement Binary Subtype Function support
object BSONFunction extends BSONBinaryBase {
  def extract(implicit stream: InputStream) = 
    throw new InvalidBSONException("BSON Functions are currently unsupported in this implementation")
}

object BSONUUID extends BSONBinaryBase {
  def extract(implicit stream: InputStream) = {
    val totalLen = readInt

    if (totalLen != 16) 
      throw new InvalidBSONException("Error in UUID Binary (0x03) length; required a length of 16 but found '%d'.".format(totalLen))

    BSONUUIDData(readLong, readLong)
  }
}

trait BSONBinaryData

case class BSONGenericBinaryData(val data: Array[Byte]) extends BSONBinaryData
case class BSONFunctionData(val data: Array[Byte]) extends BSONBinaryData
case class BSONOldBinaryData(val data: Array[Byte]) extends BSONBinaryData
case class BSONUUIDData(val part1: Long, val part2: Long) extends BSONBinaryData
case class BSONMD5Data(val data: Array[Byte]) extends BSONBinaryData
case class BSONUserBinaryData(val data: Array[Byte]) extends BSONBinaryData

// TODO - Implement Binary Subtype MD5 support
object BSONMD5 extends BSONBinaryBase {
  def extract(implicit stream: InputStream) = 
    throw new InvalidBSONException("BSON MD5 data is currently unsupported in this implementation")
}

object BSONUndefined extends BSONType[Nothing] {
  val tByte = 0x06

  def extract(implicit stream: InputStream) = Nil
}

case class BSONObjectID

object BSONObjectId extends BSONType[BSONObjectID] {
  val tByte = 0x07

  // TODO - Implement me
  def extract(implicit stream: InputStream) = throw new IllegalArgumentException("ObjectID not yet implemented")
}

object BSONBoolean extends BSONType[Boolean] {
  val tByte = 0x08

  def extract(implicit stream: InputStream) = readInt match {
    case 0x00 => false
    case 0x01 => true
    case x => throw new InvalidBSONException("Invalid Boolean subtype '%s'".format(x))
  }
}





// vim: set ts=2 sw=2 sts=2 et:
