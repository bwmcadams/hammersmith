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

import org.bson.io.{ BasicOutputBuffer, OutputBuffer }

// TODO - Enforcement of Serializable types?
/**
 * You should always subclass SerializableBSONDocument or SerializableBSONList
 * depending on how you want your object to be treated.
 */
sealed trait SerializableBSONObject extends Iterable[(String, Any)] {

  val serializer: BSONSerializer

  /**
   * The keys in your object
   * Must be available, even with list
   * (They are encoded as dictionaries to BSON)
   */
  def keySet: scala.collection.Set[String]

  def encode(out: OutputBuffer) =
    serializer.encode(this, out)

  def encode(): Array[Byte] = serializer.encode(this)

  def array_?(): Boolean

  def put(k: String, v: Any): Option[Any]

  def get(k: String): Option[Any]

//  def getOrElse(k: String, default: => Any): Any
//
//  def getOrElse[T >: Any](k: String, default: => T): T
}

trait SerializableBSONDocument extends SerializableBSONObject {
  /**
   * A map representation of your object,
   * required to serialize things.
   * TODO - Should we offer some way of protecting this?
   */
  def asMap: scala.collection.Map[String, Any]

  def array_?() = false
}

/**
 * For custom objects rather than maps
 */
trait SerializableBSONCustomDocument extends SerializableBSONDocument {
  override val keySet = asMap.keySet.asInstanceOf[Set[String]]

  override def iterator = asMap.iterator
}

trait SerializableBSONList extends SerializableBSONObject {

  override def array_?() = true
  /**
   * A sequence representation of your object
   */
  def asList: Seq[Any]

//  val keySet = asList.indices.map(_.toString).toSet

  def iterator = new Iterator[(String, Any)] {
    private val i = asList.iterator
    private var n = 0

    def hasNext = i.hasNext

    def next() = {
      val el = (n.toString, i.next)
      n += 1
      el
    }
  }
}