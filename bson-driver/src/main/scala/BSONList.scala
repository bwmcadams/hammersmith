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

import scala.collection._
import scala.collection.generic.{CanBuildFrom , SeqFactory}
import scala.collection.mutable.{GrowingBuilder , ListBuffer}

class BSONList extends SerializableBSONList {
  protected val _buf = ListBuffer.empty[Any]
  def self = _buf
  val listRepr = _buf

  val serializer = new DefaultBSONSerializer
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
  def newBuilder[A <: Any] = new BSONListBuilder
}

class BSONListBuilder extends GrowingBuilder[Any, BSONList](new BSONList) {
  override def +=(x: Any) = {
    elems += x.asInstanceOf[AnyRef] // boxing
    this
  }
}