/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
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

package org.bson

import org.bson.io.{ BasicOutputBuffer, OutputBuffer }
import java.io.{InputStream, ByteArrayInputStream}

/**
 * Type class base for anything you want to be serialized or deserialized 
 */
trait SerializableBSONObject[T] {

  def encode(doc: T, out: OutputBuffer)

  def encode(doc: T): Array[Byte] 

  def decode(in: InputStream): T

  def decode(bytes: Seq[Array[Byte]]): Seq[T] = for (b <- bytes) yield decode(b)
    
  def decode(b: Array[Byte]): T = decode(new ByteArrayInputStream(b))

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

  /**
   * Checks the "ok" field for result, if the command failed,
   * returns the "errmsg" field, on success returns None.
   */
  def checkBooleanCommandResult(doc: T): Option[String]

  /**
   * Extracts a document from the "value" field, if possible.
   * Should not throw; should return None if the field is missing
   * or invalid.
   */
  def getValueField(doc: T)(implicit mf : Manifest[T]): Option[T]
}

