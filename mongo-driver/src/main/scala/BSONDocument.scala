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

package com.mongodb

import org.bson._
import scala.collection.mutable.{LinkedHashMap , HashMap}

/* Placeholder for future usage
* TODO Implement me
*/
trait BSONDocument extends SerializableBSONDocument

class Document extends HashMap[String, Any] with BSONDocument {
  val serializer = new DefaultBSONSerializer
  def map = this.asInstanceOf[Map[String, Any]]
}

/**
* Needed for some tasks such as Commands to run safely.
*/
class OrderedDocument extends LinkedHashMap[String, Any] with BSONDocument {
  val serializer = new DefaultBSONSerializer
  def map = this.asInstanceOf[Map[String, Any]]
}

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
