
package codes.bytes

package object hammersmith {
    def hexValue(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString
}
