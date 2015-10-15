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

package codes.bytes.hammersmith.collection.immutable

import codes.bytes.hammersmith.collection.BSONDocumentFactory

import scala.collection.immutable.MapLike
import scala.collection.mutable.{HashMap, LinkedHashMap}

trait BSONDocument extends codes.bytes.hammersmith.collection.BSONDocument with Map[String, Any]
                                                               with MapLike[String, Any, BSONDocument] {
  // todo - this is needed for now, to allow easy mutation of internal repr but unsafe.
  protected def self: scala.collection.mutable.Map[String, Any]

  override def empty: BSONDocument = Document.empty

  def get(key: String): Option[Any] = self.get(key)

  def iterator: Iterator[(String, Any)] = self.iterator

  def -(key: String): BSONDocument = {
    self -= key
    this
  }

  def +[B1 >: Any](kv: (String, B1)): Map[String, B1] = add(kv)

  def add(kv: (String, Any)): BSONDocument = {
    self += kv
    this
  }

  def add(key: String, value: Any): BSONDocument = {
    self += key -> value
    this
  }

  /**
   * Convert this BSONDocument to an immutable representation
   *
   */
  def toDocument = this
}


class Document extends BSONDocument {
  protected val self = new HashMap[String, Any]
}

object Document extends BSONDocumentFactory[Document] {
  def empty = new Document
  def newBuilder: BSONDocumentBuilder[Document] = new BSONDocumentBuilder[Document](empty)
}

/**
 * Needed for some tasks such as Commands to run safely.
 */
class OrderedDocument extends BSONDocument {
  protected val self = new LinkedHashMap[String, Any]
}

object OrderedDocument extends BSONDocumentFactory[OrderedDocument] {
  def empty = new OrderedDocument
  def newBuilder: BSONDocumentBuilder[OrderedDocument] = new BSONDocumentBuilder[OrderedDocument](empty)
}

class BSONDocumentBuilder[T <: BSONDocument](empty: T) extends codes.bytes.hammersmith.collection.BSONDocumentBuilder[T](empty) {
  def +=(x: (String, Any)) = {
    elems add x
    this
  }
}
// TODO - get rid of the raw, etc code.
/**
 * A lazily evaluated BSON Document which
 * will decode it's bytestream only as needed,
 * but memoizes it once decoded for later reuse.
 *
 * You should register your own custom BSONCallback as needed
 * to control how the message is decoded.
 *
 * Another benefit of the laziness is you should be able to toggle this
 * out at will.
 *
 * TODO - For memory sanity should we drop the bytes as soon
 * as we decode them?
 */
abstract class LazyBSONDocument[+A](val raw: Array[Byte]) extends BSONDocument {
  ???
}


