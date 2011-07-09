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

package org.bson

import BSON._
import org.bson.io.Bits
import java.io._
import org.bson.types._
import java.util.{ UUID, Date => JDKDate }
import org.bson.util.{ Logging }
import scala.collection.mutable.Stack
import org.bson.collection._

/**
 * Deserialization handler which is expected to turn a BSON ByteStream into
 * objects of type "T" (or a subclass thereof)
 */
trait BSONDeserializer extends BSONDecoder with Logging {
  val callback: Callback //= new DefaultBSONCallback
  abstract class Callback extends BSONCallback {

  }

  def reset() {
    callback.reset()
    _in = null
    _callback = null
  }

  def get = callback.get

  override def decode(first: Boolean = true): Int = {
    val len = _in.readInt // hmm.. should this be parend for side effects? PEDANTRY!

    if (first) _in._max = len

    log.trace("Decoding, length: %d", len)

    _callback.objectStart()
    while (decodeElement()) {
      log.trace("Decode Loop")
    }
    _callback.objectDone()

    require(_in._read == len && first,
      "Bad Data? Post-Read Lengths don't match up. Expected: %d, Got: %d".format(len, _in._read))

    len
  }

  def decodeAndFetch(in: InputStream): AnyRef = {
    log.trace("DecodeAndFetch.")
    //reset()
    log.trace("Reset.")
    try {
      decode(in, callback)
    } catch {
      case t: Throwable => log.error(t, "Failed to decode message with callback.")
    }
    log.trace("Decoded, getting.")
    val obj = get
    log.trace("Decoded, got %s.", obj)
    get
  }

  def decode(in: InputStream, callback: Callback): Int = try {
    _decode(new Input(in), callback)
  } catch {
    case ioe: IOException => {
      log.error(ioe, "OMG! PONIES!")
      throw new BSONException("Failed to decode input data.", ioe)
    }
    case t: Throwable => log.error(t, "Unexpected exception in decode"); throw t
  }

  def decode(b: Array[Byte], callback: Callback): Int = try {
    _decode(new Input(new ByteArrayInputStream(b)), callback)
  } catch {
    case ioe: IOException => throw new BSONException("Failed to decode input data.", ioe)
    case t: Throwable => log.error(t, "Unexpected exception in decode"); throw t
  }

  protected def _decode(in: Input, callback: Callback) = {
    // TODO - Pooling/reusability
    require(in._read == 0, "Invalid Read Bytes State on Input Object.")
    try {
      if (_in != null)
        throw new IllegalStateException("_in already defined; bad state.")
      else _in = in

      if (_callback != null)
        throw new IllegalStateException("_callback already defined; bad state.")
      else _callback = callback

      decode()
    } finally {
      _in = null
      _callback = null
    }
  }

  override def decodeElement(): Boolean = {
    val t = _in.read
    if (t == EOO) {
      log.trace("End of Object. ")
      false
    } else {
      val name = _in.readCStr

      log.trace("Element Decoding with Name '%s', Type '%s'", name, t)
      decodeField(name, t)
      true
    }
  }

  /**
   * Custom callback which allows users to custom decode/capture specific fields
   *
   * This is a convenient place to override if you need to handle certain fields specially...
   */
  protected def decodeField(name: String, t: Byte) =
    _getHandle(t)(name)

  protected def _rawObject() = {
    val l = Array.ofDim[Byte](4)
    _in.fill(l)
    val len = Bits.readInt(l)
    val b = Array.ofDim[Byte](len - 4)
    _in.fill(b)
    val n = Array.concat(l, b)
    n
  }

  protected val _getHandle: PartialFunction[Byte, Function1[String, Unit]] = {
    case NULL => {
      log.trace("[Get Handle] Null value.")
      _callback.gotNull _
    }
    case UNDEFINED => {
      log.trace("[Get Handle] Undefined value.")
      _callback.gotUndefined _
    }
    case BOOLEAN => {
      log.trace("[Get Handle] Boolean value.")
      _callback.gotBoolean(_: String, _in.read() > 0)
    }
    case NUMBER => {
      log.trace("[Get Handle] Double Number value.")
      _callback.gotDouble(_: String, _in.readDouble)
    }
    case NUMBER_INT => {
      log.trace("[Get Handle] Integer Number value.")
      _callback.gotInt(_: String, _in.readInt)
    }
    case NUMBER_LONG => {
      log.trace("[Get Handle] Long Number value.")
      _callback.gotLong(_: String, _in.readLong)
    }
    case SYMBOL => {
      log.trace("[Get Handle] Symbol value.")
      _callback.gotSymbol(_: String, _in.readUTF8String)
    }
    case STRING => {
      log.trace("[Get Handle] String value.")
      _callback.gotString(_: String, _in.readUTF8String)
    }
    case OID => {
      log.trace("[Get Handle] ObjectId value.")
      // ObjectIds are stored Big endian, just to make things confusing for people.
      _callback.gotObjectId(_: String, new ObjectId(_in.readIntBE, _in.readIntBE, _in.readIntBE))
    }
    case REF => {
      log.trace("[Get Handle] DBRef value.")
      _in.readInt // length of cString that follows
      val ns = _in.readCStr
      val oid = new ObjectId(_in.readInt, _in.readInt, _in.readInt)
      _callback.gotDBRef(_: String, ns, oid)
    }
    case DATE => {
      log.trace("[Get Handle] Date value.")
      _callback.gotDate(_: String, _in.readLong)
    }
    case REGEX => {
      log.trace("[Get Handle] Regular Expression value.")
      _callback.gotRegex(_: String, _in.readCStr, _in.readCStr)
    }
    case BINARY => {
      log.debug("[Get Handle] Binary value.")
      /*_binary(_: String)*/
      val totalLen = _in.readInt
      log.trace("[Bin] Total Length: %d", totalLen)
      val binType = _in.read
      log.trace("[Bin] Binary Type: %s", binType)
      val data = Array.ofDim[Byte](totalLen)
      _in.fill(data)
      _callback.gotBinary(_: String, binType, data)
    }
    case CODE => {
      log.trace("[Get Handle] Code value.")
      _callback.gotCode(_: String, _in.readUTF8String)
    }
    case CODE_W_SCOPE => {
      log.trace("[Get Handle] Code w/ scope value.")
      _in.readInt
      _callback.gotCodeWScope(_: String, _in.readUTF8String, _readBasicObject())
    }
    case ARRAY => {
      log.trace("[Get Handle] Array value.")
      _in.readInt() // total size, ignorable
      (name: String) => {
        _callback.arrayStart(name)
        while (decodeElement()) {}
        _callback.arrayDone()
      }
    }
    case OBJECT => {
      log.trace("[Get Handle] Object value.")
      _in.readInt() // total size, ignorable
      (name: String) => {
        _callback.objectStart(name)
        while (decodeElement()) {}
        _callback.objectDone()
      }
    }
    case TIMESTAMP => {
      log.trace("[Get Handle] Timestamp value.")
      val i = _in.readInt
      val time = _in.readInt
      _callback.gotTimestamp(_: String, time, i)
    }
    case MINKEY => {
      log.trace("[Get Handle] Min Key value.")
      _callback.gotMinKey(_)
    }
    case MAXKEY => {
      log.trace("[Get Handle] Max Key value.")
      _callback.gotMaxKey(_)
    }
    case default => { name: String =>
      throw new UnsupportedOperationException("No support for type '%s', name: '%s'".format(default, name))
    }

  }
  /**
   *  immutable, but we're trading off performance on pooling here
   */
  protected var _in: Input = null
  protected var _callback: Callback = null

}

/**
 * Default BSON Deserializer which produces instances of BSONDocument
 */
class DefaultBSONDeserializer extends BSONDeserializer {
  val callback = new DefaultBSONCallback

  class DefaultBSONCallback extends Callback {

    log.trace("Beginning a new DefaultBSONCallback; assembling a Builder.")

    protected var root: BSONDocument = Document.empty

    protected var stack = new Stack[BSONDocument]
    protected var nameStack = new Stack[String]

    // Create a new instance of myself
    def createBSONCallback(): BSONCallback = new DefaultBSONCallback

    def get: BSONDocument = root

    def create(array: Boolean) =
      if (array) BSONList.empty else Document.empty

    def objectStart() {
      require(stack.size == 0, "Invalid stack state; no-arg objectStart can only be called on initial decode.")
      log.trace("Beginning a new Object w/ no args.")
      objectStart(false)
    }

    def objectStart(array: Boolean) {
      log.trace("Starting a new object... As Array? %s", array)
      root = create(array)
      stack.push(root)
    }

    def objectStart(name: String) = objectStart(false, name)

    def objectStart(array: Boolean, name: String) {
      nameStack.push(name)
      val obj = create(array)
      stack.top.put(name, obj)
      log.trace("Adding '%s' to stack '%s'", obj, stack)
      stack.push(obj)
      log.trace("Added to stack '%s'", stack)
    }

    def objectDone() = {
      val obj = stack.pop()
      if (nameStack.size > 0) {
        nameStack.pop()
      } else if (stack.size > 0) throw new IllegalStateException("Invalid Stack State.")

      // run the Decoding hooks
      // TODO - Make Decoding hooks capable of slurping in BSONDocument!
      log.trace("Root set: %s", root)
      BSON.applyDecodingHooks(obj).asInstanceOf[BSONDocument]
    }

    def arrayDone() = objectDone()

    def arrayStart(name: String) {
      objectStart(true, name)
    }

    def arrayStart() {
      objectStart(true)
    }

    def reset() {
      root = Document.empty
      if (stack != null) stack.clear()
      if (nameStack != null) nameStack.clear()
    }

    // TODO - Check and test me , I think we're getting a BSONObject back not a Document
    def gotCodeWScope(name: String, code: String, scope: AnyRef) =
      put(name, new CodeWScope(code, scope.asInstanceOf[BSONObject]))

    def gotCode(name: String, code: String) = put(name, code)

    def gotUUID(name: String, part1: Long, part2: Long) = put(name, new UUID(part1, part2))

    def gotBinary(name: String, t: Byte, data: Array[Byte]) = put(name, new Binary(t, data))

    def gotBinaryArray(name: String, b: Array[Byte]) = put(name, b)

    def gotDBRef(name: String, ns: String, id: ObjectId) = put(name, Document("$ns" -> ns, "$id" -> id))

    def gotObjectId(name: String, id: ObjectId) = put(name, id)

    def gotTimestamp(name: String, time: Int, inc: Int) = put(name, new BSONTimestamp(time, inc))

    def gotRegex(name: String, pattern: String, flags: String) =
      put(name, new FlaggableRegex(pattern, BSON.regexFlags(flags)))

    def gotSymbol(name: String, v: String) = put(name, new Symbol(v))

    def gotString(name: String, v: String) = put(name, v)

    def gotDate(name: String, millis: Long) = put(name, new JDKDate(millis))

    def gotLong(name: String, v: Long) = put(name, v)

    def gotInt(name: String, v: Int) = put(name, v)

    def gotDouble(name: String, v: Double) = put(name, v)

    def gotBoolean(name: String, v: Boolean) = put(name, v)

    def gotMaxKey(name: String) = cur.put(name, "MaxKey") // ??

    def gotMinKey(name: String) = cur.put(name, "MinKey") // ??

    def gotCustomObject(name: String, value: AnyRef) = cur.put(name, value)

    def gotUndefined(name: String) { /* NOOP */ }

    def gotNull(name: String) { /* NOOP */ }

    def cur = stack.top

    def put(k: String, v: Any) = cur.put(k, BSON.applyDecodingHooks(v))

  }

}

