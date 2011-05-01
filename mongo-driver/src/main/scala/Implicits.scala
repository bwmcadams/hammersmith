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
  implicit def asSimpleGetMoreOp(f: (Long, Seq[BSONDocument]) => Unit): GetMoreRequestFuture = SimpleRequestFutures.getMore(f)
  implicit def asSimpleQueryOp[A <: Cursor](f: A => Unit): CursorQueryRequestFuture = SimpleRequestFutures.query(f)
  implicit def asSimpleFindOneOp[A <: BSONDocument](f: A => Unit): SingleDocQueryRequestFuture = SimpleRequestFutures.findOne(f)
  implicit def asSimpleWriteOp(f: (Option[AnyRef], WriteResult) => Unit): WriteRequestFuture = SimpleRequestFutures.write(f)
}

trait Imports


