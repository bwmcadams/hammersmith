package codes.bytes

import codes.bytes.hammersmith.collection.BSONDocument

package object hammersmith extends codes.bytes.hammersmith.Implicits with codes.bytes.hammersmith.Imports {
}

package hammersmith {

  trait Implicits {
  }

  trait Imports {
    def fieldSpec[A <% BSONDocument](fields: A) = if (fields.isEmpty) None else Some(fields)

    def indexName(keys: BSONDocument) = keys.mkString("_").replace("->", "").replace(" ", "_")

  }

}
