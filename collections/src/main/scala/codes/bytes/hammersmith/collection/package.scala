package codes.bytes.hammersmith

import codes.bytes.hammersmith.bson._
//import codes.bytes.hammersmith.bson.types.{BSONBinary, BSONCode, BSONCodeWScope, BSONTimestamp}

package object collection {

  abstract class ValidBSONType[T]

  // todo - refactor types for Hammersmith's
  /*object ValidBSONType {
    implicit object BSONList extends ValidBSONType[BSONList]
    implicit object Binary extends ValidBSONType[BSONBinary]
    implicit object BSONTimestamp extends ValidBSONType[BSONTimestamp]
    implicit object Code extends ValidBSONType[BSONCode]
    implicit object CodeWScope extends ValidBSONType[BSONCodeWScope]
    implicit object ObjectId extends ValidBSONType[ObjectID]
    implicit object Symbol extends ValidBSONType[Symbol]
    implicit object BSONDocument extends ValidBSONType[BSONDocument]
  }*/

  /**
    * Nice trick from Miles Sabin using ambiguity in implicit resolution to disallow Nothing
    */
  sealed trait NotNothing[A] {
    type B
  }

  object NotNothing {
    implicit val nothing = new NotNothing[Nothing] {type B = Any}

    implicit def notNothing[A] = new NotNothing[A] {type B = A}
  }

}

