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

import wire._
import org.bson.SerializableBSONObject

trait CompletableRequest[V] {
  val request: MongoClientMessage
  val future: RequestFuture[V]
}

object CompletableRequest {

  def apply[V](m: MongoClientMessage, f: RequestFuture[V]): CompletableRequest[V] = apply[V]((m, f))

  def apply[V]: PartialFunction[(MongoClientMessage, RequestFuture[V]), CompletableRequest[V]] = {
    case (q: QueryMessage, f: SingleDocQueryRequestFuture[_]) => CompletableSingleDocRequest[V](q, f)
    case (q: QueryMessage, f: CursorQueryRequestFuture[_]) => CompletableCursorRequest[V](q, f)
    case (gm: GetMoreMessage, f: GetMoreRequestFuture[_]) => CompletableGetMoreRequest[V](gm, f)
    case (w: MongoClientWriteMessage, f: WriteRequestFuture) => CompletableWriteRequest(w, f)
    case (k: KillCursorsMessage, f: NoOpRequestFuture.type) => NonCompletableWriteRequest(k, f)
    case default => throw new IllegalArgumentException("Cannot handle a CompletableRequest of '%s'".format(default))
  }
}

trait CompletableReadRequest[V] extends CompletableRequest[V] {
  type T <: V
  override val future: QueryRequestFuture[V]
  val decoder = future.decoder
}

case class CompletableSingleDocRequest[V](override val request: QueryMessage, override val future: SingleDocQueryRequestFuture[V]) extends CompletableReadRequest[V] 
case class CompletableCursorRequest[V](override val request: QueryMessage, override val future: CursorQueryRequestFuture[V]) extends CompletableReadRequest[V]
case class CompletableGetMoreRequest[V](override val request: GetMoreMessage, override val future: GetMoreRequestFuture[V]) extends CompletableReadRequest[V]
case class CompletableWriteRequest(override val request: MongoClientWriteMessage, override val future: WriteRequestFuture) extends CompletableRequest[(Option[AnyRef], WriteResult)]
case class NonCompletableWriteRequest(override val request: MongoClientMessage, override val future: NoOpRequestFuture.type) extends CompletableRequest[Unit]


