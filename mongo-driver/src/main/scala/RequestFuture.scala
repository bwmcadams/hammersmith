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

package com.mongodb
package futures

import com.mongodb.wire.{ QueryMessage, MongoMessage }
import org.bson.types.ObjectId
import scala.concurrent.SyncVar
import scala.actors._
import org.bson._
import org.bson.util.Logging

sealed trait RequestFuture {
  type T
  val body: (Option[T], FutureResult) => Unit

  var _result: Option[FutureResult] = None
  var _element: Option[T] = None

  def result = _result
  def result_=(r: FutureResult) = _result = Some(r)
  def element = _element
  def element_=(e: T) = _element = Some(e)

  def apply() {
    if (result.isEmpty) throw new IllegalStateException("No FutureResult defined.")
    body(element, result.get)
  }

  protected[futures] var completed = false
}


sealed trait QueryRequestFuture extends RequestFuture

trait CursorQueryRequestFuture extends RequestFuture {
  type T <: Cursor
}

/**
 *
 * Used for findOne and commands
 * Items which return a single document, and not a cursor
 */
trait SingleDocQueryRequestFuture extends QueryRequestFuture {
  type T <: BSONDocument
}

/**
 * Will pass any *generated* _id along with any relevant getLastError information
 * For an update, don't expect to get ObjectId
 */
trait WriteRequestFuture extends RequestFuture {
  type T <: AnyRef // ID Type
}

trait ObjectIdWriteRequestFuture extends WriteRequestFuture {
  type T = ObjectId
}


case class FutureResult(ok: Boolean, err: Option[String], n: Int)

object RequestFutures extends Logging {
  //  def request[T : Manifest](body: (Option[T], FutureResult) => Unit): RequestFuture[T] = manifest[T] match {
  //    case c: Cursor => query(body)
  //    case d: Document => command(body)
  //    case o: ObjectId => insert(body)
  //    case default => throw new IllegalArgumentException("Cannot create a request handler for '%s'", default)
  //  }

  def query[A <: Cursor](f: (Option[A], FutureResult) => Unit) =
    new CursorQueryRequestFuture {
      type T = A
      val body = f
    }

  def find[A <: Cursor](f: (Option[A], FutureResult) => Unit) = query(f)

  def command[A <: BSONDocument](f: (Option[A], FutureResult) => Unit) =
    new SingleDocQueryRequestFuture {
      type T = A
      val body = f
    }

  def findOne[A <: BSONDocument](f: (Option[A], FutureResult) => Unit) = command(f)

  def write[Id : Manifest](f: (Option[AnyRef], FutureResult) => Unit) = manifest[Id] match {
    case oid: ObjectId => {
      log.trace("ObjectId write request.")
      new ObjectIdWriteRequestFuture {
        val body = f
      }
    }
    case default => {
      log.trace("'Default' write request.")
      new WriteRequestFuture { val body = f }
    }
  }

}

