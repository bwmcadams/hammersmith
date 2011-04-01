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
package org.bson.scala
package types 

object ObjectId {

  def apply(time: (Byte, Byte, Byte, Byte), machine: (Byte, Byte, Byte),
            pid: (Byte, Byte), inc: (Byte, Byte, Byte)) = 
    new ObjectId(time, machine, pid, inc)

  def unapply(oid: ObjectId) = ((oid.time), (oid.machine), (oid.pid), (oid.inc))

}

class ObjectId(val time: (Byte, Byte, Byte, Byte), val machine: (Byte, Byte, Byte),
               val pid: (Byte, Byte), val inc: (Byte, Byte, Byte)) {

}

// vim: set ts=2 sw=2 sts=2 et:
