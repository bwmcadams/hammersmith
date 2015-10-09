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

import scala.annotation.implicitNotFound

import akka.util.{ByteIterator, ByteString}

/**
 * Type class base for anything you want to be serialized or deserialized
 */
@implicitNotFound(msg = "Cannot find SerializableBSONObject type class for ${T}")
trait SerializableBSONObject[T] {

  /**
   * A BSONParser capable of extracting T from a BSON Byte Stream.
   *
   * @return A new instance of T
   */
  def parser: BSONParser[T]

  /**
   * A BSONComposer capable of transmogrifying an instance of T into a BSON Object (document) in bytes.
   * @return
   */
  def composer: BSONComposer[T]

  /**
   * Compose this document into BSON (aka Serialize).
   *
   * This is hardcoded logic internally - if you want to be able to
   * compose your custom object of T into BSON, you need to provide
   * a BSONComposer[T] that knows how to do the job.
   *
   * @param doc
   * @return an Akka ByteString representing the document in BSON
   */
  final def compose(doc: T): ByteString = composer(doc)


  /**
   * Parse BSON, returning an instance of this Type.
   *
   * TODO - should we even expose this, or should they just provide a composer and parser?
   *
   */
  final def parse(in: ByteIterator): T = parser(in)

  /**
   * These methods are used to validate documents in certain cases.
   * They will be invoked by the system at the appropriate times and you must
   * implement them in a manner appropriate for your object to ensure proper mongo saving.
   */
  def checkObject(doc: T, isQuery: Boolean = false): Unit

  def checkKeys(doc: T): Unit

  /**
   * Provides an iterator over all of the entries in the document
   * this is crucial for composition (serialization) to work effectively
   * if you have a custom object.
   *
   * @param doc
   * @return
   */
  def iterator(doc: T): Iterator[(String, Any)]

  /**
   * Checks for an ID and generates one, returning a new doc with the id.
   * The new doc may be a mutation of the old doc, OR a new object
   * if the old doc was immutable.
   * Not all implementers will need this, but it gets invoked nonetheless
   * as a signal to BSONDocument, etc implementations to verify an id is there
   * and generate one if needed.
   *
   */
  def checkID(doc: T): T

  /**
   * Returns t
   * @param doc
   * @return
   */
  def _id(doc: T): Option[Any]
}

