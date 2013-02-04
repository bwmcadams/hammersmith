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
package hammersmith.collection.immutable

import hammersmith.collection.BSONListFactory
import hammersmith.collection

class BSONList(val underlying: Seq[Any]) extends hammersmith.collection.BSONList
                                         with scala.collection.immutable.Seq[Any] {
  def self = underlying

  def apply(v1: Int): Any = underlying.apply(v1)

  def iterator: Iterator[Any] = underlying.iterator

  def length: Int = underlying.length
}

object BSONList extends BSONListFactory[BSONList] {
  def empty = new BSONList(Seq.empty[Any])

  def newBuilder: BSONListBuilder[BSONList] = new BSONListBuilder[BSONList](empty)
}

class BSONListBuilder[T <: BSONList](empty: T) extends hammersmith.collection.BSONListBuilder[T](empty) {
  def +=(elem: Any) = {
    elems.+:(elems)
    this
  }
}