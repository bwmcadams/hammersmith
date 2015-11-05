/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package codes.bytes.hammersmith.collection.mutable

import codes.bytes.hammersmith.collection.BSONDocumentFactory
import codes.bytes.hammersmith.collection.immutable.{BSONDocument => ImmutableBSONDocument, Document => ImmutableDocument}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, LinkedHashMap, MapProxy}

trait BSONDocument extends codes.bytes.hammersmith.collection.BSONDocument
                           with mutable.Map[String, Any]
                           with mutable.MapLike[String, Any, BSONDocument] {

  protected def self: scala.collection.mutable.Map[String, Any]

  // todo
  override def empty: BSONDocument = new Document

  /**
    * Convert this BSONDocument to an immutable representation
    *
    */
  def toDocument: ImmutableBSONDocument = ImmutableDocument(toSeq: _*)

  protected def newDocument(newSelf: mutable.Map[String, Any]): BSONDocument = new BSONDocument {
    override protected def self = newSelf
  }

  override def +=(kv: (String, Any)): BSONDocument.this.type = {
    self += kv
    this
  }

  override def -=(key: String): BSONDocument.this.type = {
    self -= key
    this
  }

  override def get(key: String): Option[Any] = self.get(key)

  override def iterator: Iterator[(String, Any)] = self.iterator
}

class Document extends BSONDocument {
  override protected def self: mutable.Map[String, Any] = HashMap.empty[String, Any]
}

object Document extends BSONDocumentFactory[Document] {
  def empty = new Document

  def newBuilder: BSONDocumentBuilder[Document] = new BSONDocumentBuilder[Document](empty)
}

/**
  * Needed for some tasks such as Commands to run safely.
  */
class OrderedDocument extends BSONDocument {
  protected val _map = new LinkedHashMap[String, Any]

  def self = _map
}

object OrderedDocument extends BSONDocumentFactory[OrderedDocument] {
  def empty = new OrderedDocument

  def newBuilder: BSONDocumentBuilder[OrderedDocument] = new BSONDocumentBuilder[OrderedDocument](empty)
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

}


