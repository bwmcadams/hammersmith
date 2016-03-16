/*
 * Copyright (c) 2011-2016 Brendan McAdams <http://bytes.codes>
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

package codes.bytes.hammersmith.bson

import codes.bytes.hammersmith.bson.types.{BSONBooleanFalse, BSONBooleanTrue, BSONBoolean, BSONType}

trait BSONMarshallingBase[T] {
  type BSONPrimitiveType <: BSONType
}

trait BSONDeserializer[T] extends BSONMarshallingBase[T] {
  def toNative(bsonType: BSONPrimitiveType): T
}

trait BSONSerializer[T] extends BSONMarshallingBase[T] {
  def toBSONType(native: T): BSONPrimitiveType
}

trait BSONBooleanSerializer[T] extends BSONSerializer[T] {
  type BSONPrimitiveType = BSONBoolean
}

trait BSONBooleanDeserializer[T] extends BSONDeserializer[T] {
  type BSONPrimitiveType = BSONBoolean
}

object DefaultBSONMarshaller {

  implicit object DefaultBSONBooleanDeser extends BSONBooleanDeserializer[Boolean] {
    def toNative(bsonType: BSONBoolean) = bsonType.booleanValue
  }

  implicit object DefaultBSONBooleanSer extends BSONBooleanSerializer[Boolean] {
    def toBSONType(native: Boolean): BSONBoolean =
      if (native) BSONBooleanTrue else BSONBooleanFalse
  }
}

// vim: set ts=2 sw=2 sts=2 et:
