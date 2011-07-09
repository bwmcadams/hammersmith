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

package com.mongodb.async
package util

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConversions._
import scala.util.control.Exception._
import org.bson.util.Logging
import scala.collection.mutable.{ ArrayBuffer, Queue }

/**
 * Wrapper for Java ConcurrentLinkedQueue
 * Based on Scala's QueueProxy & Queue classes
 */
class ConcurrentQueue[T](val underlying: ConcurrentLinkedQueue[T] = new ConcurrentLinkedQueue[T]) extends Logging {
  /**
   * Access element number <code>n</code>.
   *
   *  @return  the element at index <code>n</code>.
   *  @throws UnsupportedOperationException
   */
  def apply(n: Int): T = throw new UnsupportedOperationException("Cannot peek inside this queue.")

  /**
   * Returns the length of this queue.
   * Beware that, unlike in most collections, this method is NOT a constant-time operation.
   * Because of the asynchronous nature of these queues, determining the current number of elements requires an O(n) traversal.
   */
  def length(): Int = size()

  /**
   * Returns the length of this queue.
   * Beware that, unlike in most collections, this method is NOT a constant-time operation.
   * Because of the asynchronous nature of these queues, determining the current number of elements requires an O(n) traversal.
   */
  def size(): Int = underlying.size()

  /**
   * Checks if the queue is empty.
   *
   *  @return true, iff there is no element in the queue.
   */
  def isEmpty: Boolean = underlying.isEmpty

  /**
   * Inserts a single element at the end of the queue.
   *
   *  @param  elem        the element to insert
   */
  def +=(elem: T): this.type = { underlying.add(elem); this }

  /**
   * Adds all elements provided by an iterator
   *  at the end of the queue. The elements are prepended in the order they
   *  are given out by the iterator.
   *
   *  @param  iter        an iterator
   */
  def ++=(it: TraversableOnce[T]): this.type = {
    underlying.addAll(it.toIterable)
    this
  }

  /**
   * Adds all elements to the queue.
   *
   *  @param  elems       the elements to add.
   */
  def enqueue(elems: T*): Unit = underlying.addAll(elems)

  /**
   * Returns the first element in the queue, and removes this element
   *  from the queue.
   *
   *  @return the first element of the queue.
   */
  def dequeue(): T = underlying.poll()

  /**
   * Returns the first element in the queue, or throws an error if there
   *  is no element contained in the queue.
   *
   *  @return the first element.
   */
  def front: T = {
    val front = underlying.peek
    if (front == null)
      throw new NullPointerException
    else front
  }

  /**
   * Returns all elements currently available
   *
   */
  def dequeueAll(): Seq[T] = {
    if (underlying.isEmpty)
      Seq.empty
    else {
      val res = new ArrayBuffer[T]
      while (underlying.nonEmpty) res += underlying.poll()
      res
    }
  }

  /**
   * Removes all elements from the queue. After this operation is completed,
   *  the queue will be empty.
   */
  def clear(): Unit = underlying.clear()

  /**
   * Returns an iterator over all elements on the queue.
   *
   *  @return an iterator over all queue elements.
   */
  def iterator: Iterator[T] = underlying.iterator

  override def hashCode: Int = underlying.##

  override def equals(that: Any): Boolean =
    if (that == null) false
    else that equals underlying

  override def toString: String = underlying.toString
}

object ConcurrentQueue {
  def apply[A](xs: A*): ConcurrentQueue[A] = new ConcurrentQueue[A] ++= xs
}
