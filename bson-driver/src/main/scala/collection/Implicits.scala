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
package collection

import org.bson.util._
import org.bson.io.OutputBuffer
import org.bson.types.ObjectId
import com.twitter.util.{ Future, SimplePool }
import java.io.InputStream

object `package` {
  /**
   * TODO - Replace ThreadLocal with actor pipelines?
   * TODO - Execute around pattern
   */
  val defaultSerializerPool = new ThreadLocal(new SimplePool(for (i <- 0 until 10) yield new DefaultBSONSerializer)) // todo - intelligent pooling

  val defaultDeserializerPool = new ThreadLocal(new SimplePool(for (i <- 0 until 10) yield new DefaultBSONDeserializer)) // todo - intelligent pooling

  trait SerializableBSONDocumentLike[T <: BSONDocument] extends SerializableBSONObject[T] with Logging {

    def encode(doc: T, out: OutputBuffer) = {
      log.trace("Reserving an encoder instance")
      val serializer = defaultSerializerPool().reserve()()
      log.trace("Reserved an encoder instance")
      serializer.encode(doc, out)
      serializer.done
      defaultSerializerPool().release(serializer)
    }

    def encode(doc: T): Array[Byte] = {
      log.trace("Reserving an encoder instance")
      val serializer = defaultSerializerPool().reserve()()
      log.trace("Reserved an encoder instance")
      val bytes = serializer.encode(doc)
      serializer.done
      defaultSerializerPool().release(serializer)
      bytes
    }

    def decode(in: InputStream): T = {
      val deserializer = defaultDeserializerPool().reserve()()
      val doc = deserializer.decodeAndFetch(in).asInstanceOf[T]
      log.debug("DECODED DOC: %s as %s", doc, doc.getClass)
      defaultDeserializerPool().release(deserializer)
      doc
    }

    def checkObject(doc: T, isQuery: Boolean = false) = if (!isQuery) checkKeys(doc)

    def checkKeys(doc: T) {
      // TODO - Track key and level for clear error message?
      // TODO - Tail Call optimize me?
      // TODO - Optimize... trying to minimize number of loops but can we cut the instance checks?
      for (k <- doc.keys) {
        require(!(k contains "."), "Fields to be stored in MongoDB may not contain '.', which is a reserved character. Offending Key: " + k)
        require(!(k startsWith "$"), "Fields to be stored in MongoDB may not start with '$', which is a reserved character. Offending Key: " + k)
        if (doc.get(k).isInstanceOf[BSONDocument]) checkKeys(doc(k).asInstanceOf[T])
      }
    }

    /**
     * Checks for an ID and generates one.
     * Not all implementers will need this, but it gets invoked nonetheless
     * as a signal to BSONDocument, etc implementations to verify an id is there
     * and generate one if needed.
     */
    def checkID(doc: T): T = {
      doc.get("_id") match {
        case Some(oid: ObjectId) => {
          log.debug("Found an existing OID")
          oid.notNew()
          //oid
        }
        case Some(other) => {
          log.debug("Found a non-OID ID")
          //other
        }
        case None => {
          val oid = new ObjectId()
          doc.put("_id", oid)
          log.trace("no ObjectId. Generated: %s", doc.get("_id"))
          //oid
        }
      }
      doc
    }

    def _id(doc: T): Option[AnyRef] = doc.getAs[AnyRef]("_id")
  }

  implicit object SerializableDocument extends SerializableBSONDocumentLike[Document]

  implicit object SerializableOrderedDocument extends SerializableBSONDocumentLike[OrderedDocument]

  implicit object SerializableBSONList extends SerializableBSONDocumentLike[BSONList]

}

// vim: set ts=2 sw=2 sts=2 et:
