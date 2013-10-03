/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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

package hammersmith

import hammersmith.collection._
import hammersmith.collection.Implicits._
import hammersmith.bson.SerializableBSONObject
import hammersmith.collection.immutable.Document

object `package` extends Implicits with Imports

trait Implicits {
  /**
   * We don't support mongo versions that used 4mb as their default, so set default maxBSON to 16MB
   */
  implicit val DefaultMaxBSONSize = 1024 * 1024 * 16
}

trait Imports {
  def fieldSpec[A <% BSONDocument](fields: A) = if (fields.isEmpty) None else Some(fields)
  def indexName(keys: BSONDocument) = keys.mkString("_").replace("->", "").replace(" ", "_")

}

