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

import org.bson._
import org.bson.types.ObjectId
import scala.collection.generic._
import scala.collection.mutable._
import scala.annotation.tailrec
import scala.collection.Map

trait BSONDocument extends SerializableBSONDocument with MapProxy[String, Any] {
  /**
   * as
   *
   * Works like apply(), unsafe, bare return of a value.
   * Returns default if nothing matching is found, else
   * tries to cast a value to the specified type.
   *
   * Unless you overrode it, default throws
   * a NoSuchElementException
   *
   * @param  key (String)
   * @tparam A
   * @return (A)
   * @throws NoSuchElementException
   */
  def as[A <: Any: Manifest](key: String): A = {
    require(manifest[A] != manifest[scala.Nothing],
      "Type inference failed; as[A]() requires an explicit type argument" +
      "(e.g. document.as[<ReturnType>](\"someKey\") ) to function correctly.")

    get(key) match {
      case null => default(key).asInstanceOf[A]
      case value => value.asInstanceOf[A]
    }
  }


   /** Lazy utility method to allow typing without conflicting with Map's required get() method and causing ambiguity */
   def getAs[A <: Any: Manifest](key: String): Option[A] = {
     require(manifest[A] != manifest[scala.Nothing],
       "Type inference failed; getAs[A]() requires an explicit type argument " +
       "(e.g. document.getAs[<ReturnType>](\"somegetAKey\") ) to function correctly.")

     get(key) match {
       case null => None
       case value => Some(value.asInstanceOf[A])
     }
   }

   /**
    * Utility method to emulate javascript dot notation
    * Designed to simplify the occasional insanity of working with nested objects.
    * Your type parameter must be that of the item at the bottom of the tree you specify...
    * If cast fails - it's your own fault.
    */
   def expand[A <: Any: Manifest](key: String): Option[A] = {
     require(manifest[A] != manifest[scala.Nothing], "Type inference failed; expand[A]() requires an explicit type argument " +
                                                     " (e.g. document[<ReturnType](\"someKey\") ) to function correctly.")
     @tailrec
     def _dot(dbObj: BSONDocument, key: String): Option[_] =
       if (key.indexOf('.') < 0) {
         dbObj.getAs[AnyRef](key)
       } else {
         val (pfx, sfx) = key.splitAt(key.indexOf('.'))
         dbObj.getAs[BSONDocument](pfx) match {
           case Some(base) => _dot(base, sfx.stripPrefix("."))
           case None => None
         }
       }

     _dot(this, key) match {
       case None => None
       case Some(value) => Some(value.asInstanceOf[A])
     }
   }

}

/**
 * If you want factory fun, you need to use the Map traits.  Otherwise, roll your own.
 */
trait BSONDocumentFactory[T <: BSONDocument] {
  def empty: T

  def apply[A <: String, B <: Any](elems: (A, B)*): T = (newBuilder ++= elems).result
  def apply[A <: String, B <: Any](elems: List[(A, B)]): T = apply(elems: _*)

  def newBuilder: BSONDocumentBuilder[T] = new BSONDocumentBuilder[T](empty)
}

class BSONDocumentBuilder[T <: BSONDocument](empty: T) extends Builder[(String, Any), T] {
  protected var elems: T = empty
  def +=(x: (String, Any)) = {
    elems += x
    this
  }
  def clear() { elems = empty }
  def result: T = elems
}

class Document extends BSONDocument {
  protected val _map = new HashMap[String, Any]
  val serializer = new DefaultBSONSerializer
  def asMap = _map
  def self = _map
}

object Document extends BSONDocumentFactory[Document] {
  def empty = new Document
}

/**
 * Needed for some tasks such as Commands to run safely.
 */
class OrderedDocument extends BSONDocument {
  protected val _map = new LinkedHashMap[String, Any]
  val serializer = new DefaultBSONSerializer
  def asMap = _map
  def self = _map
}

object OrderedDocument extends BSONDocumentFactory[OrderedDocument] {
  def empty = new OrderedDocument
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
