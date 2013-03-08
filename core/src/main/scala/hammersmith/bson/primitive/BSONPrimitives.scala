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

import hammersmith.bson._
import hammersmith.collection.BSONDocument

/**
 * A Container for BSON Primitive types
 *
 */
sealed trait BSONPrimitive {
  /**
   * The "Native" type – what we call it in Scala.
   */
  type Native <: Any

  /**
   * The "container" type for this...
   * basically the Scala side primitives
   * represented.
   * For example, a BSONDateTimePrimitive's Primitive
   * is a Long , holding the milliseconds
   *
   * This is, essentially, the MOST Native type representation
   * rather than a specific instantiation.
   */
  type Primitive <: Any

  /**
   * The type represented by this primitive
   */
  def typeCode: Byte

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: Native): Primitive

  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Primitive): Native

}
// EOO is not a valid type container ;)

trait BSONDoublePrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Double
  val typeCode = BSONDoubleType.typeCode
}

trait BSONStringPrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = String
  val typeCode = BSONStringType.typeCode
}

trait BSONDocumentPrimitive[T] extends BSONPrimitive {
  type Native = T
  // Best way to represent this is as a Native set of Keys/Values
  type Primitive = Seq[(String, Any)]
  val typeCode = BSONDocumentType.typeCode
}

trait BSONArrayPrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Seq[Any]
  val typeCode = BSONArrayType.typeCode
}

sealed trait BSONBinaryPrimitive extends BSONPrimitive {
  val typeCode = BSONBinaryType.typeCode
  def subtypeCode: Byte
}

trait BSONGenericBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
  type Primitive = Array[Byte]
  val subtypeCode = BSONBinaryType.Binary_Generic
}

trait BSONBinaryFunctionPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
  type Primitive = Array[Byte] // TODO - Better type repr?
  val subtypeCode = BSONBinaryType.Binary_Function
}

trait BSONOldBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
  type Primitive = Array[Byte]
  val subtypeCode = BSONBinaryType.Binary_Old
}

trait BSONUUIDPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
  type Primitive = BSONBinaryUUID
  val subtypeCode = BSONBinaryType.Binary_UUID
}

trait BSONOldUUIDPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
  type Primitive = BSONBinaryUUID
  val subtypeCode = BSONBinaryType.Binary_UUID_Old
}

/**
 * TODO - MD5 Support. I don't know how on the JVM to deserialize one.
 */

/**
 * Use this for your own custom binarys, BSONBinaryPrimitive is
 * sealed for safety/sanity
 */
trait BSONCustomBinaryPrimitive[T] extends BSONBinaryPrimitive {
  type Native = T
}

// TODO - we have no real reason for a 'custom' objectid
trait BSONObjectIdPrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = (Int /* time */, Int /* machineID */, Int /* Increment */)
  val typeCode = BSONObjectIDType.typeCode
}

trait BSONBooleanPrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Boolean
  val typeCode = BSONBooleanType.typeCode
}

trait BSONDateTimePrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Long /* the UTC milliseconds since the Unix Epoch. */
  val typeCode = BSONDateTimeType.typeCode
}

trait BSONRegexPrimitive[T] extends BSONPrimitive {
  type Native = T
  val typeCode = BSONRegExType.typeCode
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

trait BSONJSCodePrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = String
  val typeCode = BSONJSCodeType.typeCode
}

trait BSONSymbolPrimitive[T] extends BSONPrimitive {
  type Native = T
  /** Stored as a string */
  type Primitive = String
  val typeCode = BSONSymbolType.typeCode
}

trait BSONScopedJSCodePrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = (String /* code */, org.bson.BSONObject /* Scope */)
  val typeCode = BSONScopedJSCodeType.typeCode
}

trait BSONInt32Primitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Int
  val typeCode = BSONInt32Type.typeCode
}

trait BSONTimestampPrimitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = (Int /* Time */, Int /* Increment */)
  val typeCode = BSONTimestampType.typeCode
}

trait BSONInt64Primitive[T] extends BSONPrimitive {
  type Native = T
  type Primitive = Long /* Yes, Billy. We represent Longs as Longs! */
  val typeCode = BSONInt64Type.typeCode
}

/** No hard representation of MinKey and MaxKey */

