package codes.bytes.hammersmith.akka

import codes.bytes.hammersmith.akka.bson._
import codes.bytes.hammersmith.bson.primitive.MongoObjectID$
import codes.bytes.hammersmith.collection.BSONDocument
import codes.bytes.hammersmith.collection.immutable.OrderedDocument
import com.typesafe.scalalogging.StrictLogging

// todo fix me so we chain down implicits into immutable/mutable
package object collection {

  trait SerializableBSONDocumentLike[T <: BSONDocument] extends SerializableBSONObject[T] with StrictLogging {

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
        case Some(oid: MongoObjectID) ⇒ {
          logger.debug("Found an existing OID")
          oid
        }
        case Some(other) ⇒ {
          logger.debug("Found a non-OID ID")
          other
        }
        case None ⇒ {
          // TODO - Replace me with new ObjectID Implementation
          val oid = MongoObjectID()
          logger.trace(s"no ObjectId. Generated: ${doc.get("_id")}")
          doc + "_id" -> oid
        }
      }
      doc
    }


    def _id(doc: T): Option[Any] = doc.get("_id")

    /**
      * Provides an iterator over all of the entries in the document
      * this is crucial for composition (serialization) to work effectively
      * if you have a custom object.
      *
      * @param doc
      * @return
      */
    def iterator(doc: T) = doc.iterator

  }


  // todo - can we do this with Object instead of inside implicits?
  implicit object SerializableBSONDocument extends SerializableBSONDocumentLike[BSONDocument] {
    val parser = GenericBSONDocumentParser
    val composer = GenericBSONDocumentComposer
  }

  //implicit object SerializableBSONList extends SerializableBSONDocumentLike[BSONList]

  implicit object SerializableImmutableDocument extends SerializableBSONDocumentLike[codes.bytes.hammersmith.collection.immutable.Document] {
    val parser = ImmutableBSONDocumentParser
    val composer = ImmutableBSONDocumentComposer
  }

  implicit object SerializableImmutableOrderedDocument extends SerializableBSONDocumentLike[codes.bytes.hammersmith.collection.immutable.OrderedDocument] {
    val parser: BSONParser[OrderedDocument] = ImmutableOrderedBSONDocumentParser
    val composer = ImmutableOrderedBSONDocumentComposer
  }

  //implicit object SerializableImmutableBSONList extends SerializableBSONDocumentLike[hammersmith.collection.immutable.BSONList]

  /*
  implicit object SerializableMutableDocument extends SerializableBSONDocumentLike[codes.bytes.hammersmith.collection.mutable.Document]{
    val parser = MutableBSONDocumentParser
    val composer = MutableBSONDocumentComposer
  }

  implicit object SerializableMutableOrderedDocument extends SerializableBSONDocumentLike[codes.bytes.hammersmith.collection.mutable.OrderedDocument]{
    val parser = MutableOrderedBSONDocumentParser
    val composer = MutableOrderedBSONDocumentComposer
  }

  */
  //implicit object SerializableMutableBSONList extends SerializableBSONDocumentLike[hammersmith.collection.mutable.BSONList]
}
