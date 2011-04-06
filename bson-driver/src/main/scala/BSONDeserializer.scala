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

import org.bson.util.Logging
import BSON._
import java.lang.String
import java.io._
import org.bson.types.ObjectId

trait BSONDeserializer extends BSONDecoder with Logging {
  val callback: Callback //= new DefaultBSONCallback
  abstract class Callback extends BSONCallback {
    /*
     * Reset the state.  Whereever possible, we reuse callbacks
     * to reduce overhead
     */
    reset()


  }


  override def decode(first: Boolean = true): Int = {
    val len = _in.readInt // hmm.. should this be parend for side effects? PEDANTRY!

    if (first) _in._max = len

    log.trace("Decoding, length: %d", len)

    _callback.objectStart()
    while ( decodeElement()) {}
    _callback.objectDone()

    require(_in._read == len && first,
            "Bad Data? Post-Read Lengths don't match up. Expected: %d, Got: %d".format(len, _in._read))

    len
  }

  def decode(in: InputStream , callback: Callback): Int = try {
   _decode(new Input(in), callback)
  } catch {
    case ioe: IOException => throw new BSONException("Failed to decode input data.", ioe)
  }


  def decode(b: Array[Byte] , callback: Callback): Int = try {
    _decode(new Input(new ByteArrayInputStream(b)), callback)
  } catch {
    case ioe: IOException => throw new BSONException("Failed to decode input data.", ioe)
  }

  protected def _decode(in: Input, callback: Callback) = {
    // TODO - Pooling/reusability
    require(in._read == 0, "Invalid Read Bytes State on Input Object.")
    try {
      if (_in == null)
        throw new IllegalStateException("_in already defined; bad state.")
      else _in = in

      if (_callback == null)
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
    }

    val name = _in.readCStr

    log.trace("Element Decoding with Name '%s', Type '%s'", name, t)
    _getHandle(t)(name)
    true
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
    // TODO - Ordering Ok?
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
      log.trace("[Get Handle] Binary value.")
      _binary _
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
  * TODO - immutable, but we're trading off performance on pooling here
  */
  protected var _in: Input = null
  protected var _callback: Callback = null
}

abstract class DefaultBSONDeserializer extends BSONDeserializer {
  abstract class DefaultBSONCallback extends Callback {

  }
}

