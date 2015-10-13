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

package codes.bytes.hammersmith.bson
package primitive

import codes.bytes.hammersmith.bson.types.BSONBinaryUUID
import codes.bytes.hammersmith.collection.immutable.{Document => ImmutableDocument, DBList => ImmutableDBList, OrderedDocument => ImmutableOrderedDocument}
import codes.bytes.hammersmith.collection.mutable.{Document => MutableDocument, DBList => MutableDBList, OrderedDocument => MutableOrderedDocument}
import codes.bytes.hammersmith.collection.BSONDocument

// Mostly provided as examples... we hardcode where possible for performance.

// todo - should/can these by value classes?
object DefaultBSONDoublePrimitive extends BSONDoublePrimitive[Double] {
  /**
   * The bson "container" value, from the native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: Double) = native

  /**
   * The "native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Double) = bson
}


object DefaultBSONStringPrimitive extends BSONStringPrimitive[String] {
  /**
   * The bson "container" value, from the native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: String) = native

  /**
   * The "native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: String) = bson
}

object ImmutableBSONDocumentPrimitive extends BSONDocumentPrimitive[ImmutableDocument] {
  /**
   * The bson "container" value, from the native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: ImmutableDocument) = native.toSeq

  /**
   * The "native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Seq[(String, Any)]) = ImmutableDocument(bson: _*)
}

object MutableBSONDocumentPrimitive extends BSONDocumentPrimitive[MutableDocument] {
  /**
   * The bson "container" value, from the native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: MutableDocument) = native.toSeq

  /**
   * The "native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Seq[(String, Any)]) = MutableDocument(bson: _*)
}

object ImmutableDBListPrimitive extends BSONArrayPrimitive[ImmutableDBList] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Seq[Any]) = ImmutableDBList(bson: _*)

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: ImmutableDBList) = native.toSeq
}

object MutableDBListPrimitive extends BSONArrayPrimitive[MutableDBList] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Seq[Any]) = MutableDBList(bson: _*)

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: MutableDBList) = native.toSeq
}

// TODO - Binary handlers.

object DefaultBSONUUIDPrimitive extends BSONUUIDPrimitive[java.util.UUID] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: BSONBinaryUUID) = new java.util.UUID(bson.mostSignificant, bson.leastSignificant)

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: java.util.UUID) = new BSONBinaryUUID(native.getMostSignificantBits, native.getLeastSignificantBits)
}

object DefaultBSONOldUUIDPrimitive extends BSONOldUUIDPrimitive[java.util.UUID] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: BSONBinaryUUID) = new java.util.UUID(bson.mostSignificant, bson.leastSignificant)

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: java.util.UUID) = new BSONBinaryUUID(native.getMostSignificantBits, native.getLeastSignificantBits)
}

// TODO - JodaTime
object DefaultBSONDateTimePrimitive extends BSONDateTimePrimitive[java.util.Date] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Long) = new java.util.Date(bson)

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: java.util.Date) = native.getTime
}

object DefaultInt32Primitive extends BSONInt32Primitive[Int] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Int) = bson

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: Int) = native
}

object DefaultInt64Primitive extends BSONInt64Primitive[Long] {
  /**
   * The "Native" type, read from the BSON Primitive
   *
   * e.g. BSON Integer -> JVM Int
   */
  def fromBSON(bson: Long) = bson

  /**
   * The bson "container" value, from the Native type
   *
   * e.g. Int -> BSON Representation of an Int
   *
   */
  def toBSON(native: Long) = native
}
