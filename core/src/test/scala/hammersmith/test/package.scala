
package hammersmith

package object test {
    def hexValue(buf: Array[Byte]): String = buf.map("%02X|" format _).mkString
}
