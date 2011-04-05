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
import org.bson.io.{BasicOutputBuffer , OutputBuffer}
import java.lang.String
import org.bson.BSON._

trait BSONSerializer extends BSONEncoder with Logging {

  def encode(obj: SerializableBSONObject, out: OutputBuffer) {
    set(out)
    putObject(obj)
    done()
  }

  def encode(obj: SerializableBSONObject): Array[Byte] = {
    val buf = new BasicOutputBuffer
    encode(obj, buf)
    buf.toByteArray
  }

  /**
   * Encodes a SerializableBSONObject into a BSONObject (or it's wire equivelant)
   * @param o the Object to encode
   * @return The number of characters which were encoded
   */
  def putObject(o: SerializableBSONObject): Int = putObject(None, o)
  /**
   * Encodes a SerializableBSONObject into a BSONObject (or it's wire equivelant)
   * Primarily for embedded objects, puts them by name.
   * @param name Field name to place it in.  If None, nulls on the wire.
   * @param o the Object to encode
   * @return The number of characters which were encoded
   */
  def putObject(name: Option[String], o: SerializableBSONObject): Int = {
    require(o != null, "Cannot serialize null objects.")
    log.debug("PutObject {name: '%s', value: '%s', # keys: %d", name.getOrElse("null"), o, o.keySet.size)

    val start = _buf.getPosition

    // TODO - De null me. Null BAD! Everytime you use null, someone drowns a basket full of adorable puppies
    if (handleSpecialObjects(name.getOrElse(null), o)) _buf.getPosition - start

    val rewriteID = o match {
      case list: SerializableBSONList => {
        if (name.isDefined) _put(ARRAY, name.get)
        log.trace("List Object.  Name: %s", name.getOrElse("'null'"))
        false
      }
      case obj: SerializableBSONDocument => {
        log.trace("Document Object.  Name: %s", name.getOrElse("'null'"))
        if (name.isDefined) {
          _put(OBJECT, name.get)
          if (obj.map.contains("_id")) {
            log.trace("Contains '_id', rewriting.")
            _putObjectField("_id", obj.map("_id").asInstanceOf[AnyRef])
            true
          }
        }
        false
      }
    }


    val sizePos = _buf.getPosition
    _buf.writeInt(0) // placeholder for document length

    // TODO - Support for transient fields like in the Java driver? Or does the user handle these?

    for ((k, v) <- o if k != "_id" && !rewriteID)  {
      log.trace("Key: %s, Value: %s", k, v)
      _putObjectField(k, v.asInstanceOf[AnyRef]) // force boxing
    }

    _buf.write(EOO)

    // Backtrack and set the length
    val sz = _buf.getPosition - sizePos
    log.debug("Size of Document: %d", sz)
    _buf.writeInt(sizePos, sz)
    // total bytes written
    _buf.getPosition - start
  }

  def putList(name: Option[String], obj: SerializableBSONObject): Int

  // TODO - Implement me! (Really on Mongo side of the wall)
  def putSpecial(name: String , o: SerializableBSONObject): Boolean = false
  def handleSpecialObjects(name: String , o: SerializableBSONObject): Boolean = false
}