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
import org.bson.Document

trait Cursor extends Seq[Document]

trait RequestFuture[T] {
  val body: (Option[T], FutureResult) => Unit
  protected[futures] var completed = false
}

/**
 * Also used for getMore
 */
trait QueryRequestFuture extends RequestFuture[Cursor]

/**
 * Also used for findOne
 */
trait CommandRequestFuture extends RequestFuture[Document]

/**
 * Will pass the ObjectId along with any relevant getLastError information
 */
trait InsertRequestFuture extends RequestFuture[ObjectId]

trait FutureResult {
  val ok: Boolean
  val err: Option[String]
  val n: Int
}

object RequestFutures {
  // TODO - Type Class selector
  //  def request[T : Manifest](body: (Option[T], FutureResult) => Unit): RequestFuture[T] = manifest[T] match {
  //    case c: Cursor => query(body)
  //    case d: Document => command(body)
  //    case o: ObjectId => insert(body)
  //    case default => throw new IllegalArgumentException("Cannot create a request handler for '%s'", default)
  //  }

  def query(f: (Option[Cursor], FutureResult) => Unit) = new QueryRequestFuture { self => val body = f }
  def command(f: (Option[Document], FutureResult) => Unit) = new CommandRequestFuture { self => val body = f }
  def insert(f: (Option[ObjectId], FutureResult) => Unit) = new InsertRequestFuture { self => val body = f }
}

