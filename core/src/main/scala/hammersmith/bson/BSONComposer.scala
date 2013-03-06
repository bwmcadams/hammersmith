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
package hammersmith.bson

import hammersmith.util.Logging
import akka.util.ByteString
import hammersmith.bson.primitive.BSONPrimitive

trait BSONComposer[T] extends Logging {

  private def hexDump(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString

  /**
   *
   * The lookup table for "Type" to "Primitive" conversions.
   *
   * NOTE: For concurrency, sanity, etc this should be statically composed
   * at instantiation time of your BSONComposer.
   *
   * I'm leaving it open ended for you to play, but mutate instances at your own risk.
   */
  def primitives: Map[Class[_], BSONPrimitive]

  /**
   * Encodes a document of type T down into BSON via a ByteString
   * This method is concrete, in that it invokes "composeDocument",
   * which you'll need to provide a concrete version of for T.
   *
   * @param doc
   */

}

