/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package codes.bytes.hammersmith.wire

import codes.bytes.hammersmith.collection.BSONDocument

/**
  * @param ok
  * @param error
  * @param n
  * @param code
  * @param upsertID
  * @param updatedExisting
  */
case class WriteResult(ok: Boolean,
                       error: Option[String] = None,
                       n: Int = 0,
                       upsertID: Option[AnyRef] = None,
                       updatedExisting: Option[Boolean] = None
                      )


object WriteResult {
  /**
    * { updatedExisting: true, n: 1, connectionId: 594, err: null, ok: 1.0 }
    */
  def apply(doc: BSONDocument): WriteResult = {
    val ok = doc.getAs[Int]("ok") match {
      case None =>
        false
      case Some(0) =>
        false
      case Some(1) =>
        true
    }
    val error = doc.getAs[String]("err")
    val n = doc.getAs[Int]("n") match {
      case None => 0
      case Some(_n) => _n
    }
    val upsert = doc.getAs[AnyRef]("upserted")
    val updatedExisting = doc.getAs[Boolean]("updatedExisting")
    WriteResult(ok, error, n, upsert, updatedExisting)
  }

}
