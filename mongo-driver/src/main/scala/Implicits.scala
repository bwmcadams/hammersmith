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

import org.bson.collection._
import com.mongodb.async.futures._
import org.bson.SerializableBSONObject

object `package` extends Implicits with Imports

trait Implicits {
  implicit def asGetMoreOp[T: SerializableBSONObject](f: Either[Throwable, (Long, Seq[T])] => Unit) = RequestFutures.getMore(f)
  implicit def asQueryOp[T: SerializableBSONObject](f: Either[Throwable, Cursor[T]] => Unit) = RequestFutures.query(f)
  implicit def asFindOneOp[T: SerializableBSONObject: Manifest](f: Either[Throwable, T] => Unit) = RequestFutures.findOne(f)
  implicit def asFindAndModifyOp[T: SerializableBSONObject: Manifest](f: Either[Throwable, Option[T]] => Unit) = RequestFutures.findAndModify(f)
  implicit def asWriteOp(f: Either[Throwable, (Option[AnyRef], WriteResult)] => Unit) = RequestFutures.write(f)
  implicit def asBatchWriteOp(f: Either[Throwable, (Option[Seq[AnyRef]], WriteResult)] => Unit) = RequestFutures.batchWrite(f)
  implicit def asSimpleGetMoreOp[T: SerializableBSONObject](f: (Long, Seq[T]) => Unit): GetMoreRequestFuture = SimpleRequestFutures.getMore(f)
  implicit def asSimpleQueryOp[T: SerializableBSONObject](f: Cursor[T] => Unit): CursorQueryRequestFuture = SimpleRequestFutures.query(f)
  implicit def asSimpleFindOneOp[T: SerializableBSONObject: Manifest](f: T => Unit): SingleDocQueryRequestFuture = SimpleRequestFutures.findOne(f)
  implicit def asSimpleFindAndModifyOp[T: SerializableBSONObject: Manifest](f: Option[T] => Unit): FindAndModifyRequestFuture = SimpleRequestFutures.findAndModify(f)
  implicit def asSimpleWriteOp(f: (Option[AnyRef], WriteResult) => Unit): WriteRequestFuture = SimpleRequestFutures.write(f)
  implicit def asSimpleBatchWriteOp(f: (Option[Seq[AnyRef]], WriteResult) => Unit): BatchWriteRequestFuture = SimpleRequestFutures.batchWrite(f)
  implicit def noopSimpleWrite(f: Unit): WriteRequestFuture = new WriteRequestFuture {
    val body = (result: Either[Throwable, (Option[AnyRef], WriteResult)]) => result match {
      case Right((oid, wr)) => {}
      case Left(t) => {}
    }
    override def toString = "{NoopWriteRequestFuture}"
  }

}

trait Imports {
  def fieldSpec[A <% BSONDocument](fields: A) = if (fields.isEmpty) None else Some(fields)
  def indexName(keys: BSONDocument) = keys.mkString("_").replace("->", "").replace(" ", "_")

  /**
   * Converts a standard Single Doc Command Result into a Boolean or,
   * tosses an exception if necessary.
   * @throws MongoException
   */
  protected[mongodb] def boolCmdResult[A <: BSONDocument](doc: A, throwOnError: Boolean = true): Boolean = doc.get("ok") match {
    case Some(1.0) => {
      true
    }
    case Some(_) | None => {
      if (throwOnError) throw new MongoException("Bad Boolean Command Result: %s  / %s".format(
        doc, doc.getAsOrElse[String]("errmsg", "")))
      else false
    }
  }

  protected[mongodb] def boolCmdResultCallback(callback: (Boolean) => Unit, throwOnError: Boolean = true) =
    RequestFutures.command((result: Either[Throwable, Document]) => result match {
      case Right(doc) => {
        callback(boolCmdResult(doc, throwOnError))
      }
      case Left(t) => {
        // TODO - Extract error number, if any is included
        if (throwOnError) throw new MongoException("Command Failed.", Some(t)) else callback(false)
      }
    })

}

