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
package futures

import org.bson.collection._
import org.bson.util.Logging

sealed trait RequestFuture {
  type T
  val body: Either[Throwable, T] => Unit

  def apply(error: Throwable) = body(Left(error))

  def apply[A <% T](result: A) = body(Right(result.asInstanceOf[T]))

  protected[futures] var completed = false
}

sealed trait QueryRequestFuture extends RequestFuture

trait CursorQueryRequestFuture extends RequestFuture {
  type T <: Cursor

}

trait GetMoreRequestFuture extends RequestFuture {
  type DocType <: BSONDocument
  type T = (Long, Seq[DocType])
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
  type T <: (Option[AnyRef] /* ID Type */ , WriteResult)
}

/*
 * For Noops that don't return anything such as OP_KILL_CURSORS
 */
case object NoOpRequestFuture extends RequestFuture with Logging {
  type T = Unit
  val body = (result: Either[Throwable, Unit]) => result match {
    case Right(()) => {}
    case Left(t) => log.error(t, "NoOp Command Failed.")
  }
}

object RequestFutures extends Logging {
  //  def request[T : Manifest](body: (Option[T], WriteResult) => Unit): RequestFuture[T] = manifest[T] match
  //    case c: Cursor => query(body)
  //    case d: Document => command(body)
  //    case o: ObjectId => insert(body)
  //    case default => throw new IllegalArgumentException("Cannot create a request handler for '%s'", default)
  //  }
  def getMore(f: Either[Throwable, (Long, Seq[BSONDocument])] => Unit) =
    new GetMoreRequestFuture {
      val body = f
    }

  def query[A <: Cursor](f: Either[Throwable, A] => Unit) =
    new CursorQueryRequestFuture {
      type T = A
      val body = f
    }

  def find[A <: Cursor](f: Either[Throwable, A] => Unit) = query(f)

  def command[A <: BSONDocument](f: Either[Throwable, A] => Unit) =
    new SingleDocQueryRequestFuture {
      type T = A
      val body = f
    }

  def findOne[A <: BSONDocument](f: Either[Throwable, A] => Unit) = command(f)

  def write(f: Either[Throwable, (Option[AnyRef], WriteResult)] => Unit) =
    new WriteRequestFuture {
      val body = f
    }
}

/**
 * "Simpler" request futures which swallow any errors.
 */
object SimpleRequestFutures extends Logging {
  def findOne[A <: BSONDocument](f: A => Unit) = command(f)

  def command[A <: BSONDocument](f: A => Unit) =
    new SingleDocQueryRequestFuture {
      type T = A
      val body = (result: Either[Throwable, A]) => result match {
        case Right(doc) => f(doc)
        case Left(t) => log.error(t, "Command Failed.")
      }
    }

  def getMore(f: (Long, Seq[BSONDocument]) => Unit) =
    new GetMoreRequestFuture {
      val body = (result: Either[Throwable, (Long, Seq[BSONDocument])]) => result match {
        case Right((cid, docs)) => f(cid, docs)
        case Left(t) => log.error(t, "GetMore Failed."); throw t
      }
    }

  def find[A <: Cursor](f: A => Unit) = query(f)

  def query[A <: Cursor](f: A => Unit) =
    new CursorQueryRequestFuture {
      type T = A
      val body = (result: Either[Throwable, A]) => result match {
        case Right(cursor) => f(cursor)
        case Left(t) => log.error(t, "Query Failed."); throw t
      }
    }

  def write(f: (Option[AnyRef], WriteResult) => Unit) =
    new WriteRequestFuture {
      val body = (result: Either[Throwable, (Option[AnyRef], WriteResult)]) => result match {
        case Right((oid, wr)) => f(oid, wr)
        case Left(t) => log.error(t, "Command Failed.")
      }
    }

}
