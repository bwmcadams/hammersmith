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

object `package` extends Implicits with Imports

trait Implicits {
  implicit def asGetMoreOp(f: Either[Throwable, (Long, Seq[BSONDocument])] => Unit) = RequestFutures.getMore(f)
  implicit def asQueryOp[A <: Cursor](f: Either[Throwable, A] => Unit) = RequestFutures.query(f)
  implicit def asFindOneOp[A <: BSONDocument](f: Either[Throwable, A] => Unit) = RequestFutures.findOne(f)
  implicit def asWriteOp(f: Either[Throwable, (Option[AnyRef], WriteResult)] => Unit) = RequestFutures.write(f)
  implicit def asBatchWriteOp(f: Either[Throwable, (Option[Seq[AnyRef]], WriteResult)] => Unit) = RequestFutures.batchWrite(f)
  implicit def asSimpleGetMoreOp(f: (Long, Seq[BSONDocument]) => Unit): GetMoreRequestFuture = SimpleRequestFutures.getMore(f)
  implicit def asSimpleQueryOp[A <: Cursor](f: A => Unit): CursorQueryRequestFuture = SimpleRequestFutures.query(f)
  implicit def asSimpleFindOneOp[A <: BSONDocument](f: A => Unit): SingleDocQueryRequestFuture = SimpleRequestFutures.findOne(f)
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
  def indexName(keys: BSONDocument) = keys.keys.mkString("_")

  /**
   * Converts a standard Single Doc Command Result into a Boolean or,
   * tosses an exception if necessary.
   * @throws MongoException
   */
  protected[mongodb] def boolCmdResult[A <: BSONDocument](doc: A, throwOnError: Boolean = true): Boolean = doc.getAs[Double]("ok") match {
    case Some(1.0) => {
      println("Some doc: " + doc)
      true
    }
    case Some(_) | None => {
      println("None or Non-1 doc: " + doc)
      if (throwOnError) throw new MongoException("Bad Boolean Command Result: %s  / %s".format(
                                                  doc, doc.getAsOrElse[String]("errmsg", ""))
                                                 ) else false
    }
  }

  protected[mongodb] def boolCmdResultCallback[A <: BSONDocument](callback: (Boolean) => Unit, throwOnError: Boolean = true) =
    RequestFutures.command((result: Either[Throwable, A]) => result match {
      case Right(doc) => {
        println("Right-Doc: " + doc)
        callback(boolCmdResult(doc, throwOnError))
      }
      case Left(t) => {
        println("Throwable: %s", t)
        // TODO - Extract error number, if any is included
        if (throwOnError) throw new MongoException("Command Failed.", Some(t)) else callback(false)
      }
    })

}


