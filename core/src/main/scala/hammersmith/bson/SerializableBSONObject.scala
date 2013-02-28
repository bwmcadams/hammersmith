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

import scala.annotation.implicitNotFound

import akka.util.{ByteIterator, ByteString}

/**
 * Type class base for anything you want to be serialized or deserialized
 */
@implicitNotFound(msg = "Cannot find SerializableBSONObject type class for ${T}")
trait SerializableBSONObject[T] {

  def compose(doc: T): ByteString

  @deprecated("You should pass ByteIterators for sanity/safety, not ByteStrings.")
  def parse(in: ByteString): T = parse(in.iterator)

  def parse(in: ByteIterator): T

  /**
   * These methods are used to validate documents in certain cases.
   * They will be invoked by the system at the appropriate times and you must
   * implement them in a manner appropriate for your object to ensure proper mongo saving.
   */
  def checkObject(doc: T, isQuery: Boolean = false): Unit

  def checkKeys(doc: T): Unit

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

  def _id(doc: T): Option[AnyRef]
}

