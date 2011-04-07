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
 * See the License for the specific language goverhttp://scalathon.org/projects.htmlning permissions and
 * limitations under the License.
 *
 */

package org.bson
package util

import scala.collection.generic._
import scala.collection.mutable._

/**
 * Simple utility class, adds a few conveniences on top of LinkedList
 */
class Stack[A]() extends LinearSeq[A] with GenericTraversableTemplate[A, Stack]
  with LinkedListLike[A, Stack[A]] {

  next = this

  def this(elem: A, next: Stack[A]) {
    this()
    if (next != null) {
      this.elem = elem
      this.next = next
    }
  }

  def this(elem: A) {
    this()
    this.elem = elem
  }

  override def companion: GenericCompanion[Stack] = Stack

  /**
   * Appends an item to the end of the LinkedList
   */
  def setLast(elem: A) {
    next = if (next != null) next ++ Stack(elem) else Stack(elem)
  }

  def removeLast() {
    next = next.take(next.size - 1)
  }

}

object Stack extends SeqFactory[Stack] {
  override def empty[A]: Stack[A] = new Stack[A]
  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, Stack[A]] = new GenericCanBuildFrom[A]

  def newBuilder[A]: Builder[A, Stack[A]] =
    (new MutableList) mapResult ((l: MutableList[A]) => new Stack)

}