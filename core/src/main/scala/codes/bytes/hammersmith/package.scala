/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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

package codes.bytes

import codes.bytes.hammersmith.collection.BSONDocument

package object hammersmith extends codes.bytes.hammersmith.Implicits with codes.bytes.hammersmith.Imports {
}

package hammersmith {

  trait Implicits {
  }

  trait Imports {
    def fieldSpec[A <% BSONDocument](fields: A) = if (fields.isEmpty) None else Some(fields)

    def indexName(keys: BSONDocument) = keys.mkString("_").replace("->", "").replace(" ", "_")

  }

}
