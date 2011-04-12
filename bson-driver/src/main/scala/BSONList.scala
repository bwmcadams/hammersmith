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

import scala.collection.generic.{CanBuildFrom , SeqFactory}
import scala.collection.mutable._

class BSONList extends SerializableBSONList with BSONDocument {
  override def array_?() = true
  protected val _map = new LinkedHashMap[String, Any]
  def self = _map
  val serializer = new DefaultBSONSerializer
  def asMap = _map

  def sortedKeys = _map.keys.toList.sortWith(_ < _)

  def +=(elem1: Any, elem2: Any, elems: Any*): this.type = {
    +=(elem1)
    +=(elem2)
    for (x <- elems) +=(x)
    this
  }

  def +=(elem: Any): this.type = {
    super.+=((sortedKeys.last + 1, elem): (String, Any))
    this
  }

  def -=(x: Any) = {
    remove(x)
    this
  }

  def append(elems: Any*) { for (x <- elems) +=(x) }

  def appendAll(xs: TraversableOnce[Any]) { for (x <- xs) +=(x) }

  def apply(n: Int): Any = apply(n.toString)

  def remove(x: Any): Any = {
    val elem = find { _._2 == x }.get
    super.remove(elem._1)
    elem._2
  }

  def update(n: Int, newElem: Any) { update(n.toString, newElem) }


  // TODO - Prepend, insert, put at specific index etc
}

object BSONList {
  def empty: BSONList = new BSONList

  def apply[A](elems: A*): BSONList = {
    val b = newBuilder[A]
    for (xs <- elems) xs match {
      case p: (String, _) => b += Document(p)
      case _ => b += xs
    }
    b.result
  }

  def concat[A](xss: Traversable[A]*): BSONList = {
    val b = newBuilder[A]
    if (xss forall  (_.isInstanceOf[IndexedSeq[_]])) b.sizeHint(xss map (_.size) sum)
    for (xs <- xss) b ++= xs
    b.result
  }
  def newBuilder[A <: Any] = new BSONListBuilder[BSONList](empty)
}

class BSONListBuilder[T <: BSONList](empty: T) extends Builder[Any, T] {
  protected var elems: T = empty
  def +=(x: Any) = {
    elems.+=(x)
    this
  }
  def clear() { elems = empty }
  def result: T = elems
}
