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
package collection

import hammersmith.util._
import org.bson.io.OutputBuffer
import org.bson.types.ObjectId
import java.io.InputStream
import hammersmith.bson.util.Logging
import hammersmith.bson.util.ThreadLocal

object `package` {
  /**
   * TODO - Replace ThreadLocal with actor pipelines?
   * TODO - Execute around pattern
   */
  def defaultSerializer = new ThreadLocal(new DefaultBSONSerializer)

  def defaultDeserializer = new ThreadLocal(new DefaultBSONDeserializer) 

  trait SerializableBSONDocumentLike[T <: BSONDocument] extends SerializableBSONObject[T] with Logging {

    def encode(doc: T, out: OutputBuffer) = {
      log.trace("Reserving an encoder instance")
      val serializer = defaultSerializer()
      log.trace("Reserved an encoder instance")
      serializer.encode(doc, out)
      serializer.done
    }

    def encode(doc: T): Array[Byte] = {
      log.trace("Reserving an encoder instance")
      val serializer = defaultSerializer()
      log.trace("Reserved an encoder instance")
      val bytes = serializer.encode(doc)
      serializer.done
      bytes
    }

    def decode(in: InputStream): T = {
      val deserializer = defaultDeserializer()
      val doc = deserializer.decodeAndFetch(in).asInstanceOf[T]
      log.debug("DECODED DOC: %s as %s", doc, doc.getClass)
      doc
    }

    def checkObject(doc: T, isQuery: Boolean = false) = if (!isQuery) checkKeys(doc)

    def checkKeys(doc: T) {
      // TODO - Track key and level for clear error message?
      // TODO - Tail Call optimize me?
      // TODO - Optimize... trying to minimize number of loops but can we cut the instance checks?
      for (k ← doc.keys) {
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
        case Some(oid: ObjectId) ⇒ {
          log.debug("Found an existing OID")
          oid.notNew()
          //oid
        }
        case Some(other) ⇒ {
          log.debug("Found a non-OID ID")
          //other
        }
        case None ⇒ {
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


}

object Implicits {

  implicit object SerializableDocument extends SerializableBSONDocumentLike[Document]

  implicit object SerializableOrderedDocument extends SerializableBSONDocumentLike[OrderedDocument]

  implicit object SerializableBSONList extends SerializableBSONDocumentLike[BSONList]
}

// vim: set ts=2 sw=2 sts=2 et:
