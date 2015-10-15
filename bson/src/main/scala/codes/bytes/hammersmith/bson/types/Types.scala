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

package codes.bytes.hammersmith.bson.types

import java.util.regex.Pattern

import codes.bytes.hammersmith.bson.ObjectID

import scala.util.matching.Regex

// todo - get pattern matching support in to match regex
class FlaggableRegex(regex: String, flags: Int, groupNames: String*) extends Regex(regex, groupNames: _*) {
  override val pattern = Pattern.compile(regex, flags)
}

// todo - should we de-ADT this for user customisation?
sealed trait BSONBinaryContainer
final case class BSONBinaryUUID(mostSignificant: Long, leastSignificant: Long) extends BSONBinaryContainer
final case class BSONBinaryMD5(bytes: Array[Byte]) extends BSONBinaryContainer
final case class BSONBinary(bytes: Array[Byte]) extends BSONBinaryContainer
final case class BSONBinaryUserDefined(bytes: Array[Byte]) extends BSONBinaryContainer


// Things that are mostly hard representations
sealed trait SpecialBSONValue
/** BSON Min Key and Max Key represent special internal types for Sharding */
case object BSONMinKey extends SpecialBSONValue
case object BSONMaxKey extends SpecialBSONValue
/** The dumbest types I've ever seen on earth */
case object BSONNull extends SpecialBSONValue
case object BSONUndef extends SpecialBSONValue

// Currently no dereferencing support, etc. (not a fan anyway)
final case class DBRef(namespace: String, oid: ObjectID)

sealed trait BSONCodeBlock
// needs a document for scope
final case class BSONCodeWScope(code: String, scope: Map[String, Any]) extends BSONCodeBlock
final case class BSONCode(code: String) extends BSONCodeBlock
final case class BSONTimestamp(time: Int, increment: Int)
