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

import org.bson.util.Logging
import org.bson.SerializableBSONObject

sealed trait RequestFuture[V] {
  type T

  val body: Either[Throwable, T] => Unit

  def apply(error: Throwable) = body(Left(error))

  def apply[A <% T](result: A) = body(Right(result.asInstanceOf[T]))

  protected[futures] var completed = false
}

sealed trait QueryRequestFuture[V] extends RequestFuture[V] {
  type DocType
  val decoder: SerializableBSONObject[DocType]
}

trait CursorQueryRequestFuture[V] extends QueryRequestFuture[V] {
  type T <: Cursor[DocType]
  //val decoder: SerializableBSONObject[DocType]
}

trait GetMoreRequestFuture[V] extends QueryRequestFuture[V] {
  type T = (Long, Seq[DocType])
  //val decoder: SerializableBSONObject[DocType]
}

/**
 *
 * Used for findOne and commands
 * Items which return a single document, and not a cursor
 */
trait SingleDocQueryRequestFuture[V] extends QueryRequestFuture[V] {
  type T = DocType
}

/**
 * Will pass any *generated* _id along with any relevant getLastError information
 * For an update, don't expect to get ObjectId
 */
trait WriteRequestFuture extends RequestFuture[(Option[AnyRef], WriteResult)] {
  type T <: (Option[AnyRef] /* ID Type */ , WriteResult)
}

/**
 * Will pass any *generated* _ids, in a Seq 
 * along with any relevant getLastError information
 * For an update, don't expect to get ObjectId
 *
 * Keep in mind, that WriteConcern behavior may be wonky if you do a batchInsert
 * I believe the behavior of MongoDB will cause getLastError to indicate the LAST error 
 * on your batch ---- not the first, or all of them.
 *
 * The WriteRequest used here returns a Seq[] of every generated ID, not a single ID
 */
trait BatchWriteRequestFuture extends RequestFuture[(Option[Seq[AnyRef]], WriteResult)] {
  type T <: (Option[Seq[AnyRef]] /* ID Type */ , WriteResult)
}

/*
 * For Noops that don't return anything such as OP_KILL_CURSORS
 */
case object NoOpRequestFuture extends RequestFuture[Unit] with Logging {
  type T = Unit
  val body = (result: Either[Throwable, Unit]) => result match {
    case Right(()) => {}
    case Left(t) => log.error(t, "NoOp Command Failed.")
  }

  override def toString = "{NoopWriteRequestFuture}"

}

object RequestFutures extends Logging {
  def getMore[A : SerializableBSONObject](f: Either[Throwable, (Long, Seq[A])] => Unit) =
    new GetMoreRequestFuture[A]{
      type DocType = A
      val body = f
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{GetMoreRequestFuture}"
    }

  def query[A : SerializableBSONObject](f: Either[Throwable, Cursor[A]] => Unit) =
    new CursorQueryRequestFuture[A] {
      type DocType = A
      type T = Cursor[A]
      val body = f
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{CursorQueryRequestFuture}"
    }

  def find[A : SerializableBSONObject](f: Either[Throwable, Cursor[A]] => Unit) = query(f)

  def command[A : SerializableBSONObject](f: Either[Throwable, A] => Unit) =
    new SingleDocQueryRequestFuture[A] {
      type DocType = A
      val body = f
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{SingleDocQueryRequestFuture}"
    }

  def findOne[A : SerializableBSONObject](f: Either[Throwable, A] => Unit) = command(f)

  def write(f: Either[Throwable, (Option[AnyRef], WriteResult)] => Unit) =
    new WriteRequestFuture {
      val body = f
      override def toString = "{WriteRequestFuture}"
    }

  def batchWrite(f: Either[Throwable, (Option[Seq[AnyRef]], WriteResult)] => Unit) =
    new BatchWriteRequestFuture {
      val body = f
      override def toString = "{WriteRequestFuture}"
    }
}

/**
 * "Simpler" request futures which swallow any errors.
 */
object SimpleRequestFutures extends Logging {
  def findOne[A : SerializableBSONObject](f: A => Unit) = command(f)

  def command[A : SerializableBSONObject](f: A => Unit) =
    new SingleDocQueryRequestFuture[A] {
      type DocType = A
      val body = (result: Either[Throwable, A]) => result match {
        case Right(doc) => f(doc)
        case Left(t) => log.error(t, "Command Failed.")
      }
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{SimpleSingleDocQueryRequestFuture}"
    }

  def getMore[A : SerializableBSONObject](f: (Long, Seq[A]) => Unit) =
    new GetMoreRequestFuture[A] {
      type DocType = A
      val body = (result: Either[Throwable, (Long, Seq[A])]) => result match {
        case Right((cid, docs)) => f(cid, docs)
        case Left(t) => log.error(t, "GetMore Failed."); throw t
      }
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{SimpleGetMoreRequestFuture}"
    }

  def find[T : SerializableBSONObject](f: Cursor[T] => Unit) = query(f)

  def query[A : SerializableBSONObject](f: Cursor[A] => Unit) =
    new CursorQueryRequestFuture[A] {
      type DocType = A
      type T = Cursor[A]
      val body = (result: Either[Throwable, Cursor[A]]) => result match {
        case Right(cursor) => f(cursor)
        case Left(t) => log.error(t, "Query Failed."); throw t
      }
      val decoder = implicitly[SerializableBSONObject[A]]
      override def toString = "{SimpleCursorQueryRequestFuture}"
    }

  def write(f: (Option[AnyRef], WriteResult) => Unit) =
    new WriteRequestFuture {
      val body = (result: Either[Throwable, (Option[AnyRef], WriteResult)]) => result match {
        case Right((oid, wr)) => f(oid, wr)
        case Left(t) => log.error(t, "Command Failed.")
      }
      override def toString = "{SimpleWriteRequestFuture}"
    }

  def batchWrite(f: (Option[Seq[AnyRef]], WriteResult) => Unit) =
    new BatchWriteRequestFuture {
      val body = (result: Either[Throwable, (Option[Seq[AnyRef]], WriteResult)]) => result match {
        case Right((oids, wr)) => f(oids, wr)
        case Left(t) => log.error(t, "Command Failed.")
      }
      override def toString = "{SimpleWriteRequestFuture}"
    }

}
