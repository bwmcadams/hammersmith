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

package hammersmith.collection

import scala.annotation.tailrec
import hammersmith.bson.util.Logging
import hammersmith.collection.immutable.{BSONDocument => ImmutableBSONDocument}
import scala.collection._
import scala.collection.mutable.Builder


trait BSONDocument extends Map[String, Any] with Logging {
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
  def as[A : NotNothing](key: String): A =  get(key) match {
    case None => default(key).asInstanceOf[A]
    case Some(value) => value.asInstanceOf[A]
  }

  /** Lazy utility method to allow typing without conflicting with Map's required get() method and causing ambiguity */
  def getAs[A : NotNothing : Manifest](key: String): Option[A] = get(key) match {
    case None => None
    case Some(value) => Some(value.asInstanceOf[A]) // recast as requested type.
  }


  def getAsOrElse[A : NotNothing : Manifest](key: String, default: => A): A = getAs[A](key) match {
    case Some(v) => v
    case None => default
  }

  /**
   * Utility method to emulate javascript dot notation
   * Designed to simplify the occasional insanity of working with nested objects.
   * Your type parameter must be that of the item at the bottom of the tree you specify...
   * If cast fails - it's your own fault.
   */
  def expand[A : NotNothing](key: String): Option[A] = {
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

  /**
   * Convert this BSONDocument to an immutable representation
   *
   */
  def toDocument: ImmutableBSONDocument
}

/**
 * If you want factory fun, you need to use the Map traits.  Otherwise, roll your own.
 */
trait BSONDocumentFactory[T <: BSONDocument] {
  def empty: T
  def newBuilder: BSONDocumentBuilder[T]

  def apply(elems: (String, Any)*): T = (newBuilder ++= elems).result

}

/*object BSONDocument extends BSONDocumentFactory[Document] {
  def empty: Document = hammersmith.collection.immutable.Document.empty

  def newBuilder = new BSONDocumentBuilder[Document](empty) {
    def +=(x: (String, Any)): this.type = {
      elems + x
      this
    }
  }
}*/

abstract class BSONDocumentBuilder[T <: BSONDocument](empty: T) extends Builder[(String, Any), T] {
  protected var elems: T = empty
  def +=(x: (String, Any)): this.type
  def clear() { elems = empty }
  def result(): T = elems
}


