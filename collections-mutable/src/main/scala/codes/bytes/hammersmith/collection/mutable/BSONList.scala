/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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
package codes.bytes.hammersmith.collection.mutable

import codes.bytes.hammersmith.collection.BSONListFactory
import codes.bytes.hammersmith.collection.immutable.{DBList => ImmutableDBList}

import scala.collection.mutable.{Buffer, Seq}

class DBList protected[collection](protected[mutable] val underlying: Buffer[Any]) extends codes.bytes.hammersmith.collection.BSONList
                                                                                     with Buffer[Any] {
  def self: Seq[Any] = underlying

  def update(idx: Int, elem: Any) { underlying.update(idx, elem) }

  def +=(elem: Any) = {
    underlying += elem
    this
  }

  def +=:(elem: Any) = {
    underlying.+=:(elem)
    this
  }

  def clear() {
    underlying.clear()
  }


  def insertAll(n: Int, elems: Traversable[Any]) {
    underlying.insertAll(n, elems)
  }

  def remove(n: Int): Any = underlying.remove(n)

  def apply(v1: Int): Any = underlying.apply(v1)

  def iterator: Iterator[Any] = underlying.iterator

  def length: Int = underlying.length

  /**
   * Converts this DBList to an Immutable DBList
   * @return an Immutable version of the current DBList
   */
  def toDBList: ImmutableDBList = new ImmutableDBList(underlying)

}

object DBList extends BSONListFactory[DBList] {
  def empty: DBList = new DBList(Buffer.empty[Any])

  def newBuilder: DBListBuilder[DBList] = new DBListBuilder[DBList](empty)
}

class DBListBuilder[T <: DBList](empty: T) extends codes.bytes.hammersmith.collection.BSONListBuilder[T](empty) {
  def +=(elem: Any) = {
    elems += elem
    this
  }
}
