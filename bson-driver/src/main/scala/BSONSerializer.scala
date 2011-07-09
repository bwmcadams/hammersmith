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
import org.bson.io.{ BasicOutputBuffer, OutputBuffer }
import java.lang.String
import org.bson.BSON._
import org.bson.types.ObjectId
import java.util.regex.Pattern
import scala.util.matching.Regex
import scalaj.collection.Imports._
import java.util.{ UUID, Date => JDKDate }
import org.bson.collection.BSONList
import scala.collection.Map

trait BSONSerializer extends BSONEncoder with Logging {

  def encode(obj: Map[String, Any], out: OutputBuffer) {
    set(out)
    putObject(obj)
    done()
  }

  def encode(obj: Map[String, Any]): Array[Byte] = {
    val buf = new BasicOutputBuffer
    encode(obj, buf)
    buf.toByteArray
  }

  /** Encoding of a pre-encoded byte array with some very basic validation */
  def encodeObject(o: Array[Byte]): Int = {
    val start = _buf.getPosition
    _buf.write(o)
    // total bytes written
    _buf.getPosition - start
  }

  /**
   * Encodes a Map[String, Any] into a BSONObject (or it's wire equivalent)
   * @param o the Object to encode
   * @return The number of characters which were encoded
   */
  def putObject(o: Map[String, Any]): Int = putObject(None, o)
  def putObject(name: String, o: Map[String, Any]): Int = putObject(Some(name), o)

  /**
   * Encodes a Map[String, Any] into a BSONObject (or it's wire equivalent)
   * Primarily for embedded objects, puts them by name.
   * @param name Field name to place it in.  If None, nulls on the wire.
   * @param o the Object to encode
   * @return The number of characters which were encoded
   */
  def putObject(name: Option[String], o: Map[String, Any]): Int = {
    require(o != null, "Cannot serialize null objects.")
    log.debug("PutObject {name: '%s', value: '%s'", name.getOrElse("null"), o)

    val start = _buf.getPosition

    // TODO - De null me. Null BAD! Everytime you use null, someone drowns a basket full of adorable puppies
    if (handleSpecialObjects(name.getOrElse(null), o)) _buf.getPosition - start

    // TODO - MASSIVELY Reduce repetiveness. Original impl. was clean but buggy; factored out larger to debug -bwm 5/30/11
    val _type = if (o.isInstanceOf[BSONList]) ARRAY else OBJECT

    name.foreach(_put(_type, _))

    val rewriteID = _type == OBJECT && name.isEmpty

    /*o match {
      case obj: SerializableBSONDocument => 
        if (rewriteID && obj.asMap.contains("_id")) {
          log.debug("Contains '_id', rewriting.")
          _putObjectField("_id", obj.asMap("_id").asInstanceOf[AnyRef])
        }
        // TODO - Support for transient fields like in the Java driver? Or should the user handle these?
    }*/

    val sizePos = _buf.getPosition
    _buf.writeInt(0) // placeholder for document length

    // TODO - Support for transient fields like in the Java driver? Or should the user handle these?
    for ((k, v) <- o) {
      log.trace("Key: %s, Value: %s", k, v)
      _putObjectField(k, v.asInstanceOf[AnyRef]) // force boxing
    }

    _buf.write(EOO)

    // Backtrack and set the length
    val sz = _buf.getPosition - sizePos
    log.debug("Size of Document: %d, %d", sizePos, sz)
    _buf.writeInt(sizePos, sz)
    // total bytes written
    _buf.getPosition - start
  }

  /**
   * Sort of unecessarily overriden from the Java side but I want to use PartialFunction for future features.
   */
  override def _putObjectField(name: String, value: AnyRef) {
    log.trace("\t Put Field '%s' - '%s'", name, value)

    value match {
      case "$where" => {
        log.trace("Where clause.")
        _put(CODE, name)
        _putValueString(value.toString)
      }
      case other => {
        log.trace("Applying Encoding Hooks")
        // Apply encoding hooks and then write whatever comes out
        _putHandle(BSON.applyEncodingHooks(value))(name)
      }
    }
  }

  /**
   * Do not taunt PartialFunction[Happy, Fun]
   *
   * Applied AFTER encoding Hooks.  Use orElse chaining to tack shit on, or
   * add your own encoding hooks.
   *
   * TODO - Hardwire common Scala types here rather than using slower Encoding Hooks like in Casbah
   */
  protected val _putHandle: PartialFunction[AnyRef, Function1[String, Unit]] = {
    case null => {
      log.trace("null value.")
      putNull(_: String)
    }
    case dt: JDKDate => {
      log.trace("(JDK) Date value.")
      putDate(_: String, dt)
    }
    case num: Number => {
      log.trace("Number value.")
      putNumber(_: String, num)
    }
    case str: String => {
      log.trace("String value.")
      putString(_: String, str)
    }
    case oid: ObjectId => {
      log.trace("ObjectId value: %s", oid)
      putObjectId(_: String, oid)
    }
    case bsonObj: BSONObject => {
      log.trace("BSONObject (the Java kind) value.")
      putObject(_: String, bsonObj)
    }
    case serBson: Map[String, Any] => {
      log.trace("Serializable BSON Object value.")
      putObject(_: String, serBson)
    }
    case bool: java.lang.Boolean => {
      log.trace("Boolean value.")
      putBoolean(_: String, bool)
    }
    case pattern: Pattern => {
      log.trace("RegEx Pattern.")
      putPattern(_: String, pattern)
    }
    case jdkMap: java.util.Map[_, _] => {
      log.trace("jDK Map value.")
      putMap(_: String, jdkMap.asScala)
    }
    case sMap: Map[_, _] => {
      log.trace("Scala Map value.")
      putMap(_: String, sMap)
    }
    case jdkIter: java.lang.Iterable[_] => {
      log.trace("JDK Iterable value.")
      putIterable(_: String, jdkIter.asScala)
    }
    case lst: scala.collection.Seq[_] => {
      log.trace("List (Seq) value.")
      putList(_: String, lst)
    }
    case sIter: Iterable[_] => {
      log.trace("Scala Iterable value.")
      putIterable(_: String, sIter)
    }
    case bArr: Array[Byte] => {
      log.trace("Byte Array value.")
      putBinary(_: String, bArr)
    }
    case bin: types.Binary => {
      log.trace("BSON Binary value.")
      putBinary(_: String, bin)
    }
    case uuid: UUID => {
      log.trace("UUID value.")
      putUUID(_: String, uuid)
    }
    case arr: Array[_] => {
      log.trace("Array (not of Bytes) value.")
      putArray(_: String, arr)
    }

    case sym: types.Symbol => {
      log.trace("BSON Symbol value.")
      putSymbol(_: String, sym)
    }
    case sym: Symbol => {
      log.trace("Scala Symbol value.")
      putSymbol(_: String, new types.Symbol(sym.name))
    }
    case tsp: types.BSONTimestamp => {
      log.trace("BSON Timestamp value.")
      putTimestamp(_: String, tsp)
    }
    case scopedCode: types.CodeWScope => {
      log.trace("BSON Code w/ Scope value.")
      putCodeWScope(_: String, scopedCode)
    }
    case code: types.Code => {
      log.trace("BSON Code (unscoped) value.")
      putCode(_: String, code)
    }
    case default => {
      // Weird case, attempt to push specials... delegate "match failed" up a level
      { name: String =>
        if (!putSpecial(name, default))
          throw new IllegalArgumentException("Cannot serialize '%s'".format(default.getClass))
      }
    }
  }

  protected def putArray(name: String, arr: Array[_]) {
    _put(ARRAY, name)
    val sizePos = _buf.getPosition
    _buf.writeInt(0) // placeholder for length
    for (i <- 0 until arr.length) _putObjectField(i.toString, arr(i).asInstanceOf[AnyRef]) //stupid JVM boxing
    _buf.write(EOO)

    val sz = _buf.getPosition - sizePos
    log.debug("Size of Array: %d", sz)
    _buf.writeInt(sizePos, sz)
  }
  protected def putList(name: String, lst: Seq[_]) {
    _put(ARRAY, name)
    val sizePos = _buf.getPosition
    _buf.writeInt(0) // placeholder for length
    for (i <- 0 until lst.length) _putObjectField(i.toString, lst(i).asInstanceOf[AnyRef]) // stupid JVM Boxing
    _buf.write(EOO)

    val sz = _buf.getPosition - sizePos
    log.debug("Size of List: %d", sz)
    _buf.writeInt(sizePos, sz)
  }

  protected def putMap(name: String, m: scala.collection.Map[_, _]) {
    _put(OBJECT, name)
    val sizePos = _buf.getPosition
    _buf.writeInt(0) // placeholder for length

    for ((k, v) <- m) {
      log.trace("Key: %s, Value: %s", k, v)
      _putObjectField(k.toString, v.asInstanceOf[AnyRef]) // force boxing
    }

    _buf.write(EOO)

    val sz = _buf.getPosition - sizePos
    log.debug("Size of Document: %d", sz)
    _buf.writeInt(sizePos, sz)
  }

  protected def putPattern(name: String, r: Regex): Unit = putPattern(name, r.pattern)

  protected def putPattern(name: String, p: Pattern) {
    _put(REGEX, name)
    _put(p.pattern)
    _put(regexFlags(p.flags))
  }

  protected def putIterable(name: String, i: Iterable[_]): Unit = putArray(name, i.toArray)

  // TODO - Implement me! (Really on Mongo side of the wall)   (DBrefs and the like...)
  def putSpecial(name: String, o: Map[String, Any]): Boolean = false
  def handleSpecialObjects(name: String, o: Map[String, Any]): Boolean = false

  /** Size of the Buffer */
  def size = _buf.size()

  def seek(bytes: Int) = {
    val pos = _buf.getPosition + bytes
    log.debug("Seeking to %d", pos)
  }

  def active = _buf == null
}

class DefaultBSONSerializer extends BSONSerializer

