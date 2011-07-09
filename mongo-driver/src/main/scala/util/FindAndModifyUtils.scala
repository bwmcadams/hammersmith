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

package com.mongodb.async
package util

import org.bson.types.ObjectId
import scala.annotation.tailrec
import org.bson.util.{ Logging, ThreadLocal }
import scala.collection.mutable._
import org.bson.collection._
import com.twitter.util.SimplePool
import com.mongodb.async.futures._
import java.io.InputStream
import org.bson.{ SerializableBSONObject, DefaultBSONDeserializer }

class FindAndModifyResult[V: SerializableBSONObject: Manifest] extends BSONDocument {
  val decoder = implicitly[SerializableBSONObject[V]]
  protected val _map = new HashMap[String, Any]
  def asMap = _map
  def self = _map

  def value: Option[V] = getAs[V]("value")
}

class NoMatchingDocumentError extends MongoException(msg = "No matching object found", code = 0)

/**
 * Special container for findAndModify command results.
 * The idea is that the 'outer' shell of the document is decoded as a Document
 * but the 'value' field is a custom type
 */
class SerializableFindAndModifyResult[V: SerializableBSONObject: Manifest] extends SerializableBSONDocumentLike[FindAndModifyResult[V]] with Logging {
  /*private val deserPool = new ThreadLocal(new SimplePool(for (i <- 0 until 10) yield new FindAndModifyBSONDeserializer)) // todo - intelligent pooling*/
  //val valueDecoder = implicitly[SerializableBSONObject[V]]
  override def decode(in: InputStream) = {
    //val deserializer = deserPool().reserve()()
    val deserializer = new FindAndModifyBSONDeserializer[V]
    val doc = deserializer.decodeResult(in)
    doc
  }
}

class FindAndModifyBSONDeserializer[V: SerializableBSONObject: Manifest] extends DefaultBSONDeserializer {
  val valueDecoder = implicitly[SerializableBSONObject[V]]

  override val callback = new FindAndModifyBSONCallback

  override def decodeField(name: String, t: Byte) = name match {
    case "value" => {
      require(t == org.bson.BSON.OBJECT, "findAndModify value field is not a BSONObject; decoding fails.")
      val _raw = _rawObject()
      // This trick of course only works if it's an object.
      val _value = valueDecoder.decode(_raw)
      callback.gotCustomObject(name, _value.asInstanceOf[AnyRef])
    }
    case default => super.decodeField(name, t)
  }

  def decodeResult(in: InputStream): FindAndModifyResult[V] = {
    log.trace("DecodeAndFetch.")
    //reset()
    log.trace("Reset.")
    try {
      decode(in, callback)
    } catch {
      case t: Throwable => log.error(t, "Failed to decode message with callback.")
    }
    val obj = get
    get.asInstanceOf[FindAndModifyResult[V]]
  }

  class FindAndModifyBSONCallback extends DefaultBSONCallback {
    root = new FindAndModifyResult[V]

    override def create(array: Boolean) =
      if (array) BSONList.empty else new FindAndModifyResult[V]

    override def reset() {
      root = new FindAndModifyResult[V]
      if (stack != null) stack.clear()
      if (nameStack != null) nameStack.clear()
    }

  }
}
// vim: set ts=2 sw=2 sts=2 et:
