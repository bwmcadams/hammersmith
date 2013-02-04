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

import scala.collection.{SeqLike, SeqProxyLike, SeqProxy}
import hammersmith.bson.util.Logging
import scala.collection.mutable.Builder
import hammersmith.collection.immutable.Document

trait BSONList extends Seq[Any]  with Logging {

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
   * @param idx (Int)
   * @tparam A
   * @return (A)
   * @throws NoSuchElementException
   */

  def as[A : NotNothing](idx: Int): A = apply(idx) match {
    case null => throw new NoSuchElementException
    case value => value.asInstanceOf[A]
  }

  /** Lazy utility method to allow typing without conflicting with Map's required get() method and causing ambiguity */
  def getAs[A : NotNothing](idx: Int): Option[A] = apply(idx) match {
    case null => None
    case value => Some(value.asInstanceOf[A])
  }

  def getAsOrElse[A : NotNothing](idx: Int, default: => A): A = getAs[A](idx) match {
    case Some(v) => v
    case None => default
  }
}

trait BSONListFactory[T <: BSONList] {
  def empty: BSONList
  def newBuilder: BSONListBuilder[T]

  def apply(elems: Any*): BSONList = {
    val b = newBuilder
    for (xs <- elems) xs match {
      case p: Tuple2[String, _] => b += Document(p)
      case _ => b += xs
    }
    b.result
  }

  def concat[A](xss: scala.Traversable[A]*): BSONList = {
    val b = newBuilder
    if (xss forall (_.isInstanceOf[IndexedSeq[_]]))
      b.sizeHint(xss map (_.size) sum)

    for (xs <- xss) b ++= xs
    b.result
  }

}

abstract class BSONListBuilder[T <: BSONList](empty: T) extends Builder[Any, T] {
  protected var elems: T = empty
  def +=(elem: Any): this.type

  def clear() { elems = empty }
  def result(): T = elems
}
