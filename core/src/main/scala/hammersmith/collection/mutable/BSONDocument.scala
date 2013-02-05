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

package hammersmith.collection.mutable

import scala.collection.mutable.{MapProxy, LinkedHashMap, HashMap}
import org.bson.{BasicBSONCallback, BSONCallback, BSONDecoder}
import hammersmith.collection.{BSONDocumentFactory}

protected[mutable] trait BSONDocument extends hammersmith.collection.BSONDocument with MapProxy[String, Any]

class Document extends  BSONDocument {
  protected val _map = new HashMap[String, Any]
  def self = _map
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

class BSONDocumentBuilder[T <: BSONDocument](empty: T) extends hammersmith.collection.BSONDocumentBuilder[T](empty) {
  def +=(x: (String, Any)) = {
    elems += x
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
abstract class LazyBSONDocument[+A](val raw: Array[Byte],
                                    val decoder: BSONDecoder,
                                    val callback: BSONCallback = new BasicBSONCallback) extends BSONDocument {

}


