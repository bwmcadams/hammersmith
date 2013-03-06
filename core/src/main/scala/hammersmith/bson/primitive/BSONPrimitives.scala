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

package hammersmith.bson.primitive

/**
 * A Container for BSON Primitive types
 *
 */
sealed trait BSONPrimitive {
  /**
   * The "raw" type – what we call it in Scala.
   */
  type Raw <: Any

  /**
   * The "container" type for this...
   * basically the Scala side primitives
   * represented.
   * For example, a BSONDatePrimitive's Primitive
   * is a Long , holding the milliseconds
   *
   * This is, essentially, the MOST RAW type representation
   * rather than a specific instantiation.
   */
  type Primitive <: Any

  /**
   * The type represented by this primitive
   */
  def typeCode: BSONTypeFlag.TypeCode

  /**
   * The bson "container" value, from the raw type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def bsonValue(raw: Raw): Primitive

  /**
   * The "raw" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def rawValue(bson: Primitive): Raw

}
// EOO is not a valid type container ;)

trait BSONDoublePrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Double
  val typeCode = BSONTypeFlag.Double
}

trait BSONStringPrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = String
  val typeCode = BSONTypeFlag.String
}

trait BSONObjectPrimitive[T] extends BSONPrimitive {
  type Raw = T
  // TODO - Hmm... this could be a Map[String, Any], no?
  type Primitive = org.bson.BSONObject
  val typeCode = BSONTypeFlag.Object
}

trait BSONArrayPrimitive[A, T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Seq[A]
  val typeCode = BSONTypeFlag.Array
}

sealed trait BSONBinaryPrimitive extends BSONPrimitive {
  val typeCode = BSONTypeFlag.Binary
  def subtypeCode: BSONBinarySubtypeFlag.SubtypeCode
}

trait BSONGenericBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
  type Primitive = Array[Byte]
  val subtypeCode = BSONBinarySubtypeFlag.Generic
}

trait BSONBinaryFunctionPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
  type Primitive = Array[Byte] // TODO - Better type repr?
  val subtypeCode = BSONBinarySubtypeFlag.Function
}

trait BSONOldBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
  type Primitive = Array[Byte]
  val subtypeCode = BSONBinarySubtypeFlag.OldBinary
}

trait BSONUUIDPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
  type Primitive = (Long /* most sig bits */, Long /* least sig bits */)
  val subtypeCode = BSONBinarySubtypeFlag.UUID
}

trait BSONOldUUIDPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
  type Primitive = (Long /* most sig bits */, Long /* least sig bits */)
  val subtypeCode = BSONBinarySubtypeFlag.OldUUID
}

/**
 * TODO - MD5 Support. I don't know how on the JVM to deserialize one.
 */

/**
 * Use this for your own custom binarys, BSONBinaryPrimitive is
 * sealed for safety/sanity
 */
trait BSONCustomBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Raw = T
}

trait BSONObjectIdPrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = (Int /* time */, Int /* machineID */, Int /* Increment */)
  val typeCode = BSONTypeFlag.ObjectId
}

trait BSONBooleanPrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Boolean
  val typeCode = BSONTypeFlag.Boolean
}

trait BSONDatePrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Long /* the UTC milliseconds since the Unix Epoch. */
  val typeCode = BSONTypeFlag.DateTime
}

trait BSONRegexPrimitive[T] extends BSONPrimitive {
  type Raw = T
  val typeCode = BSONTypeFlag.RegEx
  /**
   * [Regular expression]
   *
   * The first cstring is the regex pattern,
   * the second is the regex options string.
   *
   * Options are identified by characters,
   * which must be stored in alphabetical order.
   *
   * Valid options are:
   * 'i' for case insensitive matching,
   * 'm' for multiline matching,
   * 'x' for verbose mode,
   * 'l' to make \w, \W, etc. locale dependent,
   * 's' for dotall mode ('.' matches everything),
   * 'u' to make \w, \W, etc. match unicode.
   */
  type Primitive = (String /* pattern */, String /* flags */)
}

/**
 * Skip support for the deprecated DBPointer
 */

trait BSONCodePrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = String
  val typeCode = BSONTypeFlag.JSCode
}

trait BSONSymbolPrimitive[T] extends BSONPrimitive {
  type Raw = T
  /** Stored as a string */
  type Primitive = String
  val typeCode = BSONTypeFlag.Symbol
}

trait BSONScopedCodePrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = (String /* code */, org.bson.BSONObject /* Scope */)
  val typeCode = BSONTypeFlag.ScopedJSCode
}

trait BSONInt32Primitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Int
  val typeCode = BSONTypeFlag.Int32
}

trait BSONTimestampPrimitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = (Int /* Time */, Int /* Increment */)
  val typeCode = BSONTypeFlag.Timestamp
}

trait BSONInt64Primitive[T] extends BSONPrimitive {
  type Raw = T
  type Primitive = Long /* Yes, Billy. We represent Longs as Longs! */
  val typeCode = BSONTypeFlag.Int64
}

/** No hard representation of MinKey and MaxKey */

