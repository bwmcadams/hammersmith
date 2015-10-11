package codes.bytes.hammersmith

package object util extends codes.bytes.hammersmith.util.Imports with codes.bytes.hammersmith.util.Implicits {
  def hexValue(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString
}

package util {

  trait Imports

  trait Implicits {

  }

}
