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
package collection

import org.bson.types.ObjectId
import scala.annotation.tailrec
import org.bson.util.Logging
import scala.collection.mutable._

trait BSONDocument extends MapProxy[String, Any] with Logging {

  // The issue here is that asInstanceOf[A] doesn't use the
  // manifest and thus doesn't do anything (no runtime type
  // check). We have to use the manifest to cast by hand.
  // Surely there is an easier way to do this! If you know it,
  // please advise.
  private def checkedCast[A <: Any: Manifest](value: Any): A = {
    try {
      // I could not tell you why we have to check both ScalaObject
      // and AnyRef here, but for example
      // manifest[BSONDocument] <:< manifest[AnyRef]
      // is false.
      if (manifest[A] <:< manifest[AnyRef] ||
        manifest[A] <:< manifest[ScalaObject]) {
        // casting to a boxed type
        manifest[A].erasure.asInstanceOf[Class[A]].cast(value)
      } else {
        // casting to an Any such as Int, we need boxed types to unpack,
        // which asInstanceOf does but Class.cast does not
        val asAnyVal = manifest[A] match {
          case m if m == manifest[Byte] => value.asInstanceOf[Byte]
          case m if m == manifest[Short] => value.asInstanceOf[Short]
          case m if m == manifest[Int] => value.asInstanceOf[Int]
          case m if m == manifest[Long] => value.asInstanceOf[Long]
          case m if m == manifest[Float] => value.asInstanceOf[Float]
          case m if m == manifest[Double] => value.asInstanceOf[Double]
          case m if m == manifest[Boolean] => value.asInstanceOf[Boolean]
          case m if m == manifest[Char] => value.asInstanceOf[Char]
          case m => throw new UnsupportedOperationException("Type " + manifest[A] + " not supported by getAs, value is: " + value)
        }
        asAnyVal.asInstanceOf[A]
      }
    } catch {
      case cc: ClassCastException =>
        log.debug("Error casting " +
          value.asInstanceOf[AnyRef].getClass.getName +
          " to " +
          manifest[A].erasure.getName)
        throw cc
    }
  }

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
      case None => checkedCast[A](default(key))
      case Some(value) => checkedCast[A](value)
    }
  }

  /** Lazy utility method to allow typing without conflicting with Map's required get() method and causing ambiguity */
  def getAs[A <: Any: Manifest](key: String): Option[A] = {
    require(manifest[A] != manifest[scala.Nothing],
      "Type inference failed; getAs[A]() requires an explicit type argument " +
        "(e.g. document.getAs[<ReturnType>](\"somegetAKey\") ) to function correctly.")

    get(key) match {
      case None => None
      case Some(value) => Some(checkedCast[A](value))
    }
  }

  def getAsOrElse[A <: Any: Manifest](key: String, default: => A): A = getAs[A](key) match {
    case Some(v) => {
      v
    }
    case None => {
      default
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
      case Some(value) => Some(checkedCast[A](value))
    }
  }

  /**
   * A map representation of your object,
   * required to serialize things.
   * TODO - Should we offer some way of protecting this?
   */
  def asMap: scala.collection.Map[String, Any]
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
  def result(): T = elems
}

/**
 * Needed for some tasks such as Commands to run safely.
 */
class Document extends BSONDocument {
  protected val _map = new HashMap[String, Any]
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
  def asMap = _map
  def self = _map
}

object OrderedDocument extends BSONDocumentFactory[OrderedDocument] {

  def empty = new OrderedDocument
}

/**
 * List holder for ser/deser
 */
class BSONList extends BSONDocument {
  protected val _map = new HashMap[String, Any]
  def asMap = _map
  def self = _map
  def put(k: Int, v: Any): Option[Any] = put(k.toString, v)

  override def isDefinedAt(key: String): Boolean = isDefinedAt(asInt(key))

  override def contains(key: String): Boolean = contains(asInt(key))

  override def apply(key: String): Any = apply(asInt(key))

  override def getOrElse[B1 >: Any](key: String, default: => B1): B1 = getOrElse(asInt(key), default)

  override def get(key: String): Option[Any] = get(asInt(key))

  def asInt(key: String, err: Boolean = true): Int = try {
    Integer.parseInt(key)
  } catch {
    case e: Exception =>
      if (err)
        throw new IllegalArgumentException("BSONLists can only work with Integer representable keys, failed parsing '%s'".format(key))
      else -1
  }

  def isDefinedAt(key: Int): Boolean = super.isDefinedAt(key.toString)

  def contains(key: Int): Boolean = super.contains(key.toString)

  def apply(key: Int): Any = super.apply(key.toString)

  def getOrElse[B1 >: Any](key: Int, default: => B1): B1 = super.getOrElse(key.toString, default)

  def get(key: Int): Option[Any] = super.get(key.toString)

  override def toList = super.toList.sortWith(_._1 < _._1)

  def asList = toList.map(_._2)

  def asArray = toArray.sortWith(_._1 < _._1).map(_._2)

  def asIndexedSeq = toIndexedSeq.sortWith(_._1 < _._1).map(_._2)
}

object BSONList extends BSONDocumentFactory[BSONList] {
  def empty = new BSONList
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
