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
package codes.bytes.hammersmith.akka.bson

import java.nio.ByteOrder
import java.util.regex.Pattern
import java.util.{Date, UUID}

import _root_.akka.util.{ByteString, ByteStringBuilder}
import codes.bytes.hammersmith.bson._
import codes.bytes.hammersmith.bson.primitive.BSONPrimitive
import codes.bytes.hammersmith.collection.immutable.{DBList => ImmutableDBList, Document => ImmutableDocument, OrderedDocument => ImmutableOrderedDocument}
import codes.bytes.hammersmith.collection.mutable.{DBList => MutableDBList, Document => MutableDocument, OrderedDocument => MutableOrderedDocument}
import codes.bytes.hammersmith.collection.{BSONDocument, BSONList}
import com.typesafe.scalalogging.StrictLogging

import scala.annotation.tailrec
import scala.util.matching.Regex

trait BSONComposer[T] extends StrictLogging {


  implicit val byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

  /**
   *
   * The lookup table for "Type" to "Primitive" conversions.
   *
   * NOTE: For concurrency, sanity, etc this should be statically composed
   * at instantiation time of your BSONComposer.
   *
   * I'm leaving it open ended for you to play with, but mutate instances at your own risk.
   */
  def primitives: Map[Class[_], BSONPrimitive]

  /**
   * Produces an elements iterator from T
   */
  def elements(doc: T): Iterator[(String, Any)]

  /**
   * Encodes a document of type T down into BSON via a ByteString
   * This method is concrete, in that it invokes "composeDocument",
   * which you'll need to provide a concrete version of for T.
   *
   * // TODO - Make sure ordered documents compose in order... it's crucial for commands!
   */
  def apply(doc: T): ByteString = {
    implicit val b = ByteString.newBuilder
    val sz = composeBSONObject(None, elements(doc))
    // todo - make sure we compose the total document length ahead of it before return
    b.result()
  }

  /**
   * Write out a BSON Object as bytes.
   *
   * Note this comes after any conversions/separations etc.
   * This is just "Convert Iterator[(String, Any)] to BSON".
   *
   * Name is OPTIONAL as a Toplevel BSON Object (aka the full document)
   * will have no name, and won't write out a typecode either.
   *
   * From the BSON Spec a BSON Object (aka Document) is
   *
   *    document ::= int32 e_list "\x00"
   *
   *  int32 represents the total number of bytes comprising the following document – *inclusive* of the int32's 4 bytes
   *  e_list is a list of possible elements, where:
   *
   *    e_list ::= element e_list
   *
   *  As a sequence of elements, wherein an element is
   *
   *    element ::= typeCode fieldName typeValue
   *
   * The above of which (typeCode and typeValue) are defined for each possible BSON type.
   *
   * Field Name is a CString, not a UTF8 String
   *
   * \x00 terminates the document (somewhat redundant given a fixed length header but what the hell)..
   *  it wouldn't be so bad if \x00 wasn't used a million other places inside BSON, thereby precluding "scanahead" parsing
   *
   * For the curious, Document being embeddable, its representation in the element section of the BNF grammar is:
   *
   *    document_element ::= "\x03" e_name document
   *
   * @param fieldName Optionally (if an embedded doc) the field name for this document entry
   * @param values An Iterator of String -> Any, representing the values of the Document.
   */
  final def composeBSONObject(fieldName: Option[String], values: Iterator[(String, Any)])(implicit b: ByteStringBuilder): Int = {
    implicit val innerB = ByteString.newBuilder


    // TODO - Validate there's an ID (there's a hook in serializablebsonobject for this) and that it is written to the doc.
    // Now write out ze fields
    for ((k, v) <- values) composeField(k, v)(innerB)
    innerB += BSONEndOfObjectType.typeCode

    val hdr = innerB.length + 4 /* include int32 bytes as header*/
    /**
     * If no name, this is a toplevel object, which gets no type written
     * Otherwise, write our typecode and field name (e.g. embedded object)
     */
    fieldName match {
      case Some(name) =>
        b.putByte(BSONDocumentType.typeCode) // typeCode
        composeCStringValue(name)(b) // fieldName
      case None => // noop
    }
    b.putInt(hdr) ++= innerB.result() // not as elegant as i'd like but we need to compose on a separate inner bytestringbuilder.
    hdr
  }

  /**
   * Write out a BSON Array as bytes.
   *
   * Note this comes after any conversions/separations etc.
   * This is just "Convert Iterator[(String, Any)] to BSON".
   *
   * Fundamentally, under the covers a BSON Array is just an Int indexed BSON Document,
   * which means we will recurse back on the whole encoder as it is possible for Arrays to
   * contain other arrays, Documents, etc. So encoding of any types is dealt with elsewhee.
   *
   * From the BSON Spec a BSON Object (aka Document) is
   *
   *    document ::= int32 e_list "\x00"
   *
   *  int32 represents the total number of bytes comprising the following document – *inclusive* of the int32's 4 bytes
   *  e_list is a list of possible elements, where:
   *
   *    e_list ::= element e_list
   *
   *  As a sequence of elements, wherein an element is
   *
   *    element ::= typeCode fieldName typeValue
   *
   *  The above of which (typeCode and typeValue) are defined for each possible BSON type.
   *  For arrays, fieldName will always be an Int32.
   *
   * \x00 terminates the document (somewhat redundant given a fixed length header but what the hell)..
   *  it wouldn't be so bad if \x00 wasn't used a million other places inside BSON, thereby precluding "scanahead" parsing
   *
   * For the curious, Array has a representation in the element section of the BNF grammar is:
   *
   *    document_element ::= "\x04" e_name document
   *
   * @param key The field name to write the array into
   * @param values An Iterator of Any, representing the values of the Array
   */
  def composeBSONArray(key: String, values: Iterator[Any])(implicit b: ByteStringBuilder): Int = {
    require(values.length < Integer.MAX_VALUE, "MongoDB Arrays use Int indexing and cannot exceed MAX_INT entries.")
    implicit val innerB = ByteString.newBuilder


    // Now write out ze fields
    val (_, len) = values.foldLeft((0, 0)) { (last, entry) => (last._1 + 1, last._2 + composeField(last._1.toString, entry)(innerB)) }
    innerB += BSONEndOfObjectType.typeCode
    val hdr = innerB.length + 4 /* include int32 bytes as header*/
    b.putByte(BSONArrayType.typeCode) // typeCode
    composeCStringValue(key)(b) // fieldName
    b.putInt(hdr) ++= innerB.result() // not as elegant as i'd like but we need to compose on a separate inner bytestringbuilder.
    hdr
  }



  protected def composeField(key: String, value: Any)(implicit b: ByteStringBuilder): Int = {
    logger.trace(s"Composing field $key with value of type '${value.getClass}'")
    // todo - performance tune/tweak this
    // todo - support ancestry, possibly w/ code atlassian contributed to java driver.
    // todo - capture length
    if (primitives.isEmpty) {
      defaultFieldComposition(key, value)
    } else primitives.get(value.getClass) match {
      case Some(prim) =>
        primitiveFieldComposition(key, prim, value)
      case None =>
        // standard behavior
        defaultFieldComposition(key, value)
    }
  }

  protected def primitiveFieldComposition(key: String, primitive: BSONPrimitive, value: Any)(implicit b: ByteStringBuilder) = ???



  protected def defaultFieldComposition(key: String, value: Any)(implicit b: ByteStringBuilder) = value match {
    // things we handle as Double
    case dbl: Double =>
      composeBSONDouble(key, dbl)
    case flt: Float =>
      logger.warn(s"['$key'] Converting Float (32-bit) to a Double (64-bit) may result in some precision loss")
      composeBSONDouble(key, flt)
    /* We should NOT blindly encode BigDecimal */
    case bd: BigDecimal =>
      throw new UnsupportedOperationException("BSON Does not safely support BigDecimal storage.")
    // things we handle as string
    case str: String =>
      composeBSONString(key, str)
    // things we handle as embedded docs
    case doc: BSONDocument =>
      // todo - guarantee if this is Ordered that its iterator provides ordering guarantees on iteration
      composeBSONObject(Some(key), doc.iterator)
    case m: Map[String, Any] =>
      logger.warn(s"['$key'] Writing raw Maps to BSON is inadvisable; all values will be encoded as Any, " +
                   "and type erasure may wreak havoc. Please consider creating a BSONDocument.")
      composeBSONObject(Some(key), m.iterator)
    // Things we handle as arrays
    case lst: BSONList =>
      composeBSONArray(key, lst.iterator)
    case arr: Array[Any] =>
      logger.warn(s"['$key'] Got an Array , which due to type erasure is being treated as a BSON Array. " +
                   "If you meant to store Binary data, Please construct an instance of BSONBinaryContainer for serialization.")
      composeBSONArray(key, arr.iterator)
    case set: Set[Any] =>
      logger.warn(s"['$key'] WARNING: MongoDB does NOT provide storage guarantees around Sets, only allowing arrays to be " +
        "treated as Sets during atomic updates. Please see: http://docs.mongodb.org/manual/reference/operator/set/#_S_set")
      composeBSONArray(key, set.iterator)
    case seq: Seq[Any] =>
      composeBSONArray(key, seq.iterator)
    // Things we handle as binary data
    case u: UUID =>
      composeBSONUUID(key, u)
    // todo MD5 support
    case BSONBinary(bytes) =>
      composeBSONBinary(key, bytes, BSONBinaryType.Binary_Generic)
    case BSONBinaryUserDefined(bytes) =>
      composeBSONBinary(key, bytes, BSONBinaryType.Binary_UserDefined)
    // Things we treat as ObjectID
    case oid: ObjectID =>
      composeBSONObjectID(key, oid)
    // Things we treat as Boolean
    case bool: Boolean =>
      composeBSONBoolean(key, bool)
    // Things we treat as UTC DateTimes
    case date: Date =>
      composeBSONDateTime(key, date.getTime)
    // we aren't going to support encoding null types
    // Things we treat as Regular Expressions
    case re: Regex =>
      val p = re.pattern
      composeBSONRegex(key, p.pattern, BSONRegExType.parseFlags(p.flags))
    case p: Pattern =>
      composeBSONRegex(key, p.pattern, BSONRegExType.parseFlags(p.flags))
    // Things we treat as a DBRef NOT as a deprecated DBPointer
    case d: DBRef => ??? // TODO - Uh, we should do something other than MethodNotImpemented here... Kind of important
    // Things we treat as JSCode
    case code: BSONCode =>
      composeBSONCode(key, code.code)
    // Things we treat as Scoped JSCode
    case scoped: BSONCodeWScope =>
      composeBSONScopedCode(key, scoped.code, scoped.scope)
    // Things we treat as a Symbol
    case s: Symbol =>
      composeBSONSymbol(key, s.name)
    // Things we treat as a Int32
    case i: Int =>
      composeBSONInt32(key, i)
    // Things we treat as a Int64 (Long)
    case l: Long =>
      composeBSONInt64(key, l)
    case s: Short =>
      throw new UnsupportedOperationException("MongoDB Cannnot safely/sanely store short values.")
    // Things we treat as BSON Timestamps
    case ts: BSONTimestamp =>
      composeBSONTimestamp(key, ts.time, ts.increment)
    // BSON Min Key and Max Key
    case BSONMinKey =>
      composeBSONMinKey(key)
    case BSONMaxKey =>
      composeBSONMaxKey(key)
    case other =>
       throw new UnsupportedOperationException("Unable to serialize type '%s' (value: '%s'".format(other.getClass))
  }

  /**
   * a BSON Min Key, special semantic for sharding
   *
   *  min_key ::= "\xFF" e_name
   *
   * Special type which compares lower than all other possible BSON element values.
   */
  def composeBSONMinKey(key: String)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONMinKeyType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    len
  }

  /**
   * a BSON Max Key, special semantic for sharding
   *
   *  max_key ::= "\x7F" e_name
   *
   * Special type which compares higher than all other possible BSON element values.
   */
  def composeBSONMaxKey(key: String)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONMaxKeyType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    len
  }

  protected def composeBSONCode(key: String, code: String)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONJSCodeType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // code
    len += composeUTF8StringValue(code)
    len
  }

  protected def composeBSONScopedCode(key: String, code: String, scope: Map[String, Any])(implicit b: ByteStringBuilder): Int = {
    implicit val innerB = ByteString.newBuilder
    // type code
    b.putByte(BSONScopedJSCodeType.typeCode)
    var totalLen = 1 // with byte
    // field name
    totalLen += composeCStringValue(key)(b)
    // code
    var len = 0 //type code doesn't count
    len += composeUTF8StringValue(code)(innerB)
    len += composeBSONObject(None, scope.iterator)(innerB)
    len += 4 // (Itself)
    b.putInt(len) ++= innerB.result()
    totalLen += len // for outside, type code counts, + 4 for the internal length
    totalLen
  }

  /**
   * A BSON Timestamp, special internal type not to be confused with DateTime
   *
   *  timestamp ::= \"x11" e_name int64
   *
   *  Special internal type used by MongoDB replication and sharding.
   *  First 4 bytes are an increment, second 4 are a timestamp.
   *  Setting the timestamp to 0 has special semantics.
   */
  def composeBSONTimestamp(key: String, time: Int, increment: Int)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONTimestampType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    b.putInt(time)
    b.putInt(increment)
    len + 8 // we wrote 8 bytes total (2 x int32 @ 4 bytes per)
  }

  /**
   * A BSON Representation of a Int32
   *
   *    int32 ::= "\x10" e_name int32
   *
   */
  def composeBSONInt32(key: String, value: Int)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONInt32Type.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    b.putInt(value)
    len + 4 // int32 are 4 bytes
  }

  /**
   * A BSON Representation of a Int64
   *
   *    int64 ::= "\x12" e_name int64
   *
   */
  def composeBSONInt64(key: String, value: Long)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONInt64Type.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    b.putLong(value)
    len + 8 // int64 are 8 bytes
  }

  /**
   * A BSON Representation of a Regular Expression
   *
   *    regex ::= "\x0B" e_name cstring cstring
   *
   * The first cstring is the regex pattern, the second is the regex options string.
   * Options are identified by characters, which must be stored in alphabetical order.
   *
   *  Valid options are:
   *    'i' for case insensitive matching,
   *    'm' for multiline matching,
   *    'x' for verbose mode,
   *    'l' to make \w, \W, etc. locale dependent,
   *    's' for dotall mode ('.' matches everything),
   *    and 'u' to make \w, \W, etc. match unicode.
   */
  protected def composeBSONRegex(key: String, pattern: String, flags: String)(implicit b: ByteStringBuilder): Int = {
    // todo - validate flags stays within bounds of MongoDB supported values
    // type code
    b.putByte(BSONRegExType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // pattern
    len += composeCStringValue(pattern)
    // flags
    len += composeCStringValue(flags)
    len
  }

  /**
   * A BSON Representation of a UTC DateTime (# of UTC seconds since Unix Epoch)
   *
   *    utc_datetime ::= "\x09" e_name int64
   *
   */
  protected def composeBSONDateTime(key: String, value: Long)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONDateTimeType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    b.putLong(value)
    len + 8 // 8 bytes for a long
  }

  /**
   * A BSON Representation of a Boolean
   *
   *    boolean ::= "\x08" e_name ("\x00" for true | "\x01" for false)
   *
   */
  protected def composeBSONBoolean(key: String, value: Boolean)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONBooleanType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    b.putByte(if (value) 0x01 else 0x00)
    len + 1 // 1 byte for boolean value
  }

  /**
   *  A BSON representation of an ObjectID
   *
   *    object_id ::= "\x07" e_name (byte*12)
   *
   *  Obviously, ObjectIDs are serialized as 12 bytes
   */
  protected def composeBSONObjectID(key: String, value: ObjectID)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONObjectIDType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    b.putBytes(value.toBytes)
    len + 12 // oid serialises as 12 bytes
  }

  /**
   * A BSON Binary container represents several tyes of Bytes
   *
   *    binary ::= "\x05" e_name int32 subtype (byte*)
   *
   *  Where subtype is one of:
   *
   *    \x00 -> "Generic"/"General Purpose" binary
   *    \x01 -> A serialized function
   *    \x02 -> Old Binary - a former, now deprecated style in MongoDB
   *    \x03 -> Old UUID - some drivers used to serialize UUIDs incorrectly, this marks those.
   *    \x04 -> UUID - where the byte order is "correct"
   *    \x05 -> MD5 - MD5 hash
   *    \x80 -> User Defined Binary Data.
   *
   * At the point of this code all decomposing (like UUID -> bytes) should already be done.
   */
  protected def composeBSONBinary(key: String, value: Array[Byte], subType: Byte)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONBinaryType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    val subLen = value.length
    b.putInt(subLen)
    // subtype code
    b.putByte(subType)
    b.putBytes(value)
    len + subLen
  }

  protected def composeBSONUUID(key: String, value: UUID)(implicit b: ByteStringBuilder): Int = {
    // UUIDs are meant to be written in BIG endian order (this was broken in old drivers, hence Old UUID tag
    b.putByte(BSONBinaryType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // sub field length
    b.putInt(16)
    // subtype
    b.putByte(BSONBinaryType.Binary_UUID)
    b.putLong(value.getMostSignificantBits)(ByteOrder.BIG_ENDIAN)
    b.putLong(value.getLeastSignificantBits)(ByteOrder.BIG_ENDIAN)
    len + 16 // UUIDs are exactly 16 bytes
  }

  /**
   * A BSON Double is a 64-bit IEEE 754 floating point number.
   *
   *    double_element ::= "\x01" e_name double
   */
  protected def composeBSONDouble(key: String, value: Double)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONDoubleType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    b.putDouble(value)
    len + 8 // doubles are 64 bit - 8 bytes
  }

  /**
   * A BSON Symbol - same as String just diff. type; for immutable stuff.
   * String is encoded in UTF8 (not the cStrings sometimes used)
   *
   *    string_element ::= "\x0E" e_name string
   */
  protected def composeBSONSymbol(key: String, value: String)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONSymbolType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    len += composeUTF8StringValue(value)
    len
  }
  /**
   * A BSON String is encoded in UTF8 (not the cStrings sometimes used)
   *
   *    string_element ::= "\x02" e_name string
   */
  protected def composeBSONString(key: String, value: String)(implicit b: ByteStringBuilder): Int = {
    // type code
    b.putByte(BSONStringType.typeCode)
    var len = 1 // type code is 1
    // field name
    len += composeCStringValue(key)
    // field value
    len += composeUTF8StringValue(value)
    len
  }


  /**
   * Write the BSON Bytes of a String as a BSON CString
   * An example of what happens when stupidity overwhelms API Design ... because it's lazier to write without a length header
   *
   *  cString ::= (byte*) "\x00"
   *
   * Zero or more modified UTF-8 encoded characters followed by '\x00'. The (byte*) MUST NOT contain '\x00', hence it is not full UTF-8.
   */
  def composeCStringValue(value: String)(implicit b: ByteStringBuilder): Int = {
    require(!value.contains(0x0.toByte), "C String values for BSON may not contain 0x0 bytes.")
    composeUTF8StringValue(value, true)
  }

  /**
   * Write the BSON Bytes of a String as UTF8
   *
   * From the BSON Spec, a UTF8 String is:
   *
   *    string ::= int32 (byte*) "\x00"
   *
   *  int32 is the number of bytes in the (byte*) [+ 1 for the trailing \x00].
   *  (byte*) is zero or more UTF-8 encoded characters.
   *  \x00 trailing again, just for fun because it serves no help parsing.
   *
   * @param value The String to write as a BSON UTF8 String
   * @param b a ByteStringBuilder to attach the UTF8 String to.
   *
   *
   * I looked into other UTF8 implementations, but I am basing this on the
   * Mongo Java Driver which I suspect is encoding codepoints differently due to BSON oddities
   * TODO - Make me more bleeding efficient.
   * TODO - Test the fuck out of me (fuzzing)
   *
   */
  protected def composeUTF8StringValue(value: String, asCString: Boolean = false)(implicit b: ByteStringBuilder): Int = {
    // todo - confirm we don't exceed mongodb's/bson's maximum string length
    val len = utf8Length(value)
    // write length header
    if (!asCString)
      b.putInt(len + 1) // An extra byte in len for the trailing \x00
    @tailrec
    def write(i: Int) {
      // current byte
      val code = value.codePointAt(i)
      if (code < 0x80) {
        b.putByte(code.toByte)
      } else if (code < 0x800) {
        b.putByte((0xC0 + (code >> 6)).toByte)
        b.putByte((0x80 + (code & 0x3F)).toByte)
      } else if (code < 0x10000) {
        b.putByte((0xE0 + (code >> 12)).toByte)
        b.putByte((0x80 + ((code >> 6) & 0x3F)).toByte)
        b.putByte((0x80 + (code & 0x3F)).toByte)
      } else {
        b.putByte((0xF0 + (code >> 18)).toByte)
        b.putByte((0x80 + ((code >> 12) & 0x3F)).toByte)
        b.putByte((0x80 + ((code >> 6) & 0x3F)).toByte)
        b.putByte((0x80 + (code & 0x3F)).toByte)
      }
      val x = Character.charCount(code)
      assume(i + x <= len, "UTF8 Loop issue, exceeded total expected character length.")
      if (i + x < len) write(i + x)
    }
    write(0)
    // Last byte, the trailer.
    b.putByte(0x00.toByte)
    if (asCString)
      len + 1
    else
      len + 1 + 4 // for the header
  }

  /**
   * Calculates the length of a UTF8 String.
   *
   * // todo - i did basic tests on this, compared to hadoop impl but not against larger characters worth 2 or 3
   * @param value The String to write
   * @return An Int32 representing the length.
   */
  def utf8Length(value: String): Int = {
    @tailrec
    def calc(s: String, len: Int): Int = {
      // this byte
      val c = s.head
      // length of the next byte
      val x = if (c < 0x80) 1 else if (c < 0x800) 2 else if (c < 0x10000) 3 else 4
      if (s.tail.length == 0) len + x else calc(s.tail, len + x)
    }
    calc(value, 0)
  }
}

/**
 * Default no frills BSONDocumentComposer
 */
object GenericBSONDocumentComposer extends BSONComposer[BSONDocument] {
  def primitives = Map.empty[Class[_], BSONPrimitive]

  def elements(doc: BSONDocument) = doc.iterator
}

object ImmutableBSONDocumentComposer extends BSONComposer[ImmutableDocument] {
  def primitives = Map.empty[Class[_], BSONPrimitive]

  def elements(doc: ImmutableDocument) = doc.iterator
}

object ImmutableOrderedBSONDocumentComposer extends BSONComposer[ImmutableOrderedDocument] {
  def primitives = Map.empty[Class[_], BSONPrimitive]

  def elements(doc: ImmutableOrderedDocument) = doc.iterator
}

object MutableBSONDocumentComposer extends BSONComposer[MutableDocument] {
  def primitives = Map.empty[Class[_], BSONPrimitive]

  def elements(doc: MutableDocument) = doc.iterator
}

object MutableOrderedBSONDocumentComposer extends BSONComposer[MutableOrderedDocument] {
  def primitives = Map.empty[Class[_], BSONPrimitive]

  def elements(doc: MutableOrderedDocument) = doc.iterator
}

case class BSONCompositionException(message: String, t: Throwable = null) extends Exception(message, t) with scala.util.control.NoStackTrace