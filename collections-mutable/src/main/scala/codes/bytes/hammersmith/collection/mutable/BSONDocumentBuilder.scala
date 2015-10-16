package codes.bytes.hammersmith.collection.mutable;

class BSONDocumentBuilder[T <: BSONDocument](empty: T) extends codes.bytes.hammersmith.collection.BSONDocumentBuilder[T](empty) {
  def +=(x: (String, Any)) = {
    elems += x
    this
  }
}
