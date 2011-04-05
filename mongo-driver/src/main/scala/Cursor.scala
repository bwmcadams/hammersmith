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
package com.mongodb

import org.bson.util.Logging

/**
 * Lazy decoding Cursor for MongoDB Objects
 * Works on the idea that we're better off decoding each
 * message as iteration occurs rather than trying to decode them
 * all up front
 */
trait Cursor { // extends Stream with Logging {
  val cursorID: Long

  // TODO - A more flexible definition of this answer
  protected def tailDefined = false
}