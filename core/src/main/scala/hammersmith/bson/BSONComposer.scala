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

import hammersmith.util.Logging
import akka.util.{ByteStringBuilder, ByteString}
import hammersmith.bson.primitive.BSONPrimitive
import scala.annotation.tailrec

trait BSONComposer[T] extends Logging {

  implicit val tM: SerializableBSONObject[T]

  private def hexDump(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString

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
   * Encodes a document of type T down into BSON via a ByteString
   * This method is concrete, in that it invokes "composeDocument",
   * which you'll need to provide a concrete version of for T.
   *
   * // TODO - Make sure ordered documents compose in order... it's crucial for commands!
   */
  def apply(doc: T): ByteString = {
    val b = ByteString.newBuilder
    composeObject(None, tM.iterator(doc))
    // todo - make sure we compose the total document length ahead of it before return
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
   *  The above of which (typeCode and typeValue) are defined for each possible BSON type.
   *
   * \x00 terminates the document (somewhat redundant given a fixed length header but what the hell)..
   *  it wouldn't be so bad if \x00 wasn't used a million other places inside BSON, thereby precluding "scanahead" parsing
   *
   *
   * @param fieldName Optionally (if an embedded doc) the field name for this document entry
   * @param values An Iterator of String -> Any, representing the values of the Document.
   * @return a ByteString representing the object.
   */
  protected def composeObject(fieldName: Option[String], values: Iterator[(String, Any)]): ByteString = {
    implicit val b = ByteString.newBuilder
    /**
     * If no name, this is a toplevel object, which gets no type written
     * Otherwise, write our typecode and field name (e.g. embedded object)
     */
    fieldName match {
      case Some(name) =>
        b.putByte(BSONDocumentType.typeCode) // typeCode
        composeUTF8String(name) // fieldName
      case None => // noop
    }

    // TODO - Validate there's an ID (there's a hook in serializablebsonobject for this) and that it is written to the doc.
    // Now write out ze fields
    val len = values.foldLeft(0) { (last, kv) => last + composeField(kv._1, kv._2) }
    ByteString.newBuilder.putInt(len + 4 /* include int32 bytes */).result() ++ b.result()
  }

  protected def composeField(key: String, value: Any)(implicit b: ByteStringBuilder): Int = {
    log.debug(s"Composing field $key with value of type '" + value.getClass + "'")
    // todo - performance tune/tweak this
    // todo - support ancestry, possibly w/ code atlassian contributed to java driver.
    primitives.get(value.getClass) {
      Some(prim) =>

      None =>
        // standard behavior
    }
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
  protected def composeUTF8String(value: String)(implicit b: ByteStringBuilder) {
    // todo - confirm we don't exceed mongodb's/bson's maximum string length
    val len = utf8Length(value)
    // write length header
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
      assume(x <= len, "UTF8 Loop issue, exceeded total expected character length.")
      if (x < len) write(i + x)
    }
    // Last byte, the trailer.
    b.putByte(0x00.toByte)
  }

  /**
   * Calculates the length of a UTF8 String.
   *
   * // todo - i did basic tests on this, compared to hadoop impl but not against larger characters worth 2 or 3
   * @param value The String to write
   * @return An Int32 representing the length.
   */
  protected def utf8Length(value: String): Int = {
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

