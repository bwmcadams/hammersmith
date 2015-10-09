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

import codes.bytes.hammersmith.util.Logging
import akka.util.ByteIterator
import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.util.matching.Regex
import codes.bytes.hammersmith.collection.immutable.{Document => ImmutableDocument, DBList => ImmutableDBList, OrderedDocument => ImmutableOrderedDocument}
import codes.bytes.hammersmith.collection.mutable.{Document => MutableDocument, DBList => MutableDBList, OrderedDocument => MutableOrderedDocument}
import codes.bytes.hammersmith.collection.BSONDocument

/** T is your toplevel document type. */
trait BSONParser[T] extends Logging {
  implicit val thisParser = this

  private def hexDump(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString

  /**
   * A "Core" parse routine when you expect your input contains multiple
   * documents, such as the documents* block of a protocol ReplyMessage.
   *
   * Instead of decoding all documents ahead of time, this will amortize
   * the decode when the stream is evaluated.
   *
   * In any case where you get documents*, you should also know the count.
   */
  def asStream(count: Int, frame: ByteIterator) = {
    for (i <- (0 until count).toStream) yield apply(frame)
  }

  /** The "core" parse routine; should break down your BSON into T */
  def apply(frame: ByteIterator): T = {
    // Extract the BSON doc
    val sz = frame.len
    val len = frame.getInt(ByteOrder.LITTLE_ENDIAN)
    require(len < BSONDocumentType.MaxSize,
      "Invalid document. Expected size less than Max BSON Size of '%s'. Got '%s'".format(BSONDocumentType.MaxSize, len))
    log.debug(s"Frame Size: $sz Doc Size: $len")
    val data = frame.take(len)
    log.debug(s"Parsing a BSON doc of $len bytes, with a data block of " + data.len)
    val obj = parseRootObject(parse(data))
    log.debug(s"Parsed root object: '$obj'")
    obj
  }


  /** Parses a sequence of entries into a Root object, which must be of type T
   * Separated from parseDocument to allow for discreet subdocument types (which may backfire on me)
   */
  def parseRootObject(entries: Seq[(String, Any)]): T

  /** Parse documents... */
  @tailrec
  protected[bson] final def parse(frame: ByteIterator, entries: Queue[(String, Any)] = Queue.empty[(String, Any)]): Queue[(String, Any)] = {
    val typ = frame.head
    //log.trace("{%d} DECODING TYPE '%s' len [%d]", System.nanoTime(), typ.toByte, frame.len)
    // TODO - Big performance boost if we move this to a @switch implementation
    val _entries: Queue[(String, Any)] = frame match {
      case BSONEndOfObjectType(eoo) =>
        log.trace("Got a BSON End of Object")
        entries
      case BSONNullType(field) =>
        // TODO - how best to represent nulls / undefs?
        log.trace("Got a BSON Null for field '%s'", field)
        entries :+ (field, BSONNullType) // TODO: This isn't sane probably, but we need a proper placeholder type.
      case BSONUndefType(field) =>
        // TODO - how best to represent nulls / undefs?
        log.warn("DEPRECATED TYPE: Got a BSON Undef for field '%s'", field)
        entries :+ (field, BSONUndefType) // TODO: This isn't sane probably, but we need a proper placeholder type.
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
      case BSONDateTimeType(field, value) =>
        log.trace("Got a BSON UTC Timestamp '%s' for field '%s'", value, field)
        entries :+ (field, parseDateTime(field, value))
      case BSONRegExType(field, value) =>
        log.trace("Got a BSON Regex '%s' for field '%s'", value, field)
        entries :+ (field, parseRegEx(field, value))
      case BSONDBPointerType(field, value) =>
        log.trace("Got a BSON DBRef '%s' for field '%s'", value, field)
        // no custom parsing for dbRef until necessity is proven
        entries :+ (field, value)
      case BSONJSCodeType(field, value) =>
        log.trace("Got a BSON JSCode for field '%s'", field)
        // no custom parsing for now
        entries :+ (field, value)
      case BSONScopedJSCodeType(field, value) =>
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
        log.trace("Remaining data: " + hexDump(frame.toArray))
        throw new BSONParsingException(s"No support for decoding BSON Type of byte '$typ'/$unknown ")
      }
    if (BSONEndOfObjectType.typeCode == typ) {
      log.trace("***** EOO")
      frame.next()
      _entries
    } else parse(frame, _entries)
  }
//
//  // TEMP FOR DEBUG REMOVE ME
//  @tailrec
//  final def readCString(frame: ByteIterator, buffer: StringBuilder = new StringBuilder): String = {
//    val c = frame.next().toChar
//    //log.trace("[c] '" + c + "'")
//    if (c == 0x00) {
//      buffer.toString
//    } else {
//      buffer.append(c)
//      readCString(frame, buffer)
//    }
//  }
//  // TEMP FOR DEBUG REMOVE ME
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
  def parseDocument(field: String, values: Seq[(String, Any)]): Any = ImmutableDocument(values: _*)

 /**
   * Overridable method for how to handle adding a array entry
   * You should compose the list of entries into a List structure of some kind
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  def parseArray(field: String, values: Seq[Any]): Any = ImmutableDBList(values: _*)

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


/**
 * "Generic" BSONDocumentParser... returns an immutable document
 */
object GenericBSONDocumentParser extends BSONParser[BSONDocument] {
  def parseRootObject(entries: Seq[(String, Any)]) = ImmutableDocument(entries: _*)
}


object ImmutableBSONDocumentParser extends BSONParser[ImmutableDocument] {
  def parseRootObject(entries: Seq[(String, Any)]) = ImmutableDocument(entries: _*)
}

object ImmutableOrderedBSONDocumentParser extends BSONParser[ImmutableOrderedDocument] {
  def parseRootObject(entries: Seq[(String, Any)]) = ImmutableOrderedDocument(entries: _*)

  /**
   * Overridable method for how to handle adding a document entry
   * You should compose the lists of Key/Value pairs into a doc of some kind
   *
   * Defaults to Document from the builtin hammersmith collection library.
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  override def parseDocument(field: String, values: Seq[(String, Any)]): Any = ImmutableOrderedDocument(values: _*)

}

/* @deprecated("What the f**k is wrong with you? Reading mutable documents?") */
object MutableBSONDocumentParser extends BSONParser[MutableDocument] {
  def parseRootObject(entries: Seq[(String, Any)]) = MutableDocument(entries: _*)

  /**
   * Overridable method for how to handle adding a document entry
   * You should compose the lists of Key/Value pairs into a doc of some kind
   *
   * Defaults to Document from the builtin hammersmith collection library.
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  override def parseDocument(field: String, values: Seq[(String, Any)]): Any = MutableDocument(values: _*)

  /**
   * Overridable method for how to handle adding a array entry
   * You should compose the list of entries into a List structure of some kind
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  override def parseArray(field: String, values: Seq[Any]): Any = MutableDBList(values: _*)
}

/* @deprecated("What the f**k is wrong with you? Reading mutable documents?") */
object MutableOrderedBSONDocumentParser extends BSONParser[MutableOrderedDocument] {
  def parseRootObject(entries: Seq[(String, Any)]) = MutableOrderedDocument(entries: _*)

  /**
   * Overridable method for how to handle adding a document entry
   * You should compose the lists of Key/Value pairs into a doc of some kind
   *
   * Defaults to Document from the builtin hammersmith collection library.
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  override def parseDocument(field: String, values: Seq[(String, Any)]): Any = MutableOrderedDocument(values: _*)

  /**
   * Overridable method for how to handle adding a array entry
   * You should compose the list of entries into a List structure of some kind
   *
   * Field is provided in case you need to respond differently based
   * upon field name; should not be returned back.
   */
  override def parseArray(field: String, values: Seq[Any]): Any = MutableDBList(values: _*)
}

class BSONParsingException(message: String, t: Throwable = null) extends Exception(message, t)
