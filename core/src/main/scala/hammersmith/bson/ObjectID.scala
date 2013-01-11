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

import hammersmith.bson.Logging 

import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer 

/**
 * Globally unique ID for Objects, typically used as a primary key.
 * <p>Consists of 12 bytes, divided as follows:
 * <blockquote><pre>
 * <table border="1">
 * <tr><td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td>
 *     <td>7</td><td>8</td><td>9</td><td>10</td><td>11</td></tr>
 * <tr><td colspan="4">time</td><td colspan="3">machine</td>
 *     <td colspan="2">pid</td><td colspan="3">inc</td></tr>
 * </table>
 * </pre></blockquote>
 *
 * @see http://docs.mongodb.org/manual/core/object-id/
 */
class ObjectID private(val timestamp: Int = (System.currentTimeMillis() / 1000), 
							 				 val machineID: Int = ObjectID.generatedMachineID,
							 				 val increment: Int = ObjectID.nextIncrement(),
							 				 val isNew: Boolean = true) extends Ordered[ObjectID] with Logging {

	def compare(that: ObjectID): Int = {
		def compareUnsigned(n: Int, o: Int) = {
			val mask = 0xFFFFFFFFL
			val x = n & mask
			val y = o & mask
			val diff = x - y
			if (diff < Int.MinValue) Int.MinValue 
			else if (diff > Integer.MaxValue) Int.MaxValue
			else diff
		}

		if (that == null) -1 
		else {
			// crappy implementation, copied from Mongo Java driver.
			lazy val timeComp = compareUnsigned(this.timestamp, that.timestamp)
			lazy val machineComp = compareUnsigned(this.machineID, that.machineID)
			lazy val incrementComp = compareUnsigned(this.increment, that.increment)
			if (timeComp != 0) timeComp 
			else if (machineComp != 0) timeComp
			else incrementComp
		}
	}

	override def toBytes: Array[Byte] = {
		val bytes = new Array[Byte](12)
		val buf = ByteBuffer.wrap(bytes)
		buf.putInt(timestamp)
		buf.putInt(machineID)
		buf.putInt(increment)
		b
	}

	override def toString: String = {
		val bytes = toBytes

		val buf = new StringBuilder(24)

		for (i <- 0 until bytes.length) {
			val s = (bytes(i) & 0xFF).toHexString
			if (s.length == 1) buf.append(0)
			buf.append(s)
		}

		buf.toString
	}
}

object ObjectID extends hammersmith.bson.Logging {

	def apply() = new ObjectID()

	def apply(timestamp: Int = (System.currentTimeMillis() / 1000), 
						machineID: Int = generatedMachineID,
						increment: Int = nextIncrement(),
						isNew: Boolean = false) = 
		new ObjectID(timestamp, machineID, increment, isNew)


	def apply(time: java.util.Date, 
					 machineID: int = generatedMachineID,
					 increment: Int = nextIncrement()) = 
		new ObjectID((time.getTime / 1000).toInt, machineID, increment, false)
	
	def apply(b: Array[Byte]) = {
		require(b.length == 12, "ObjectIDs must consist of exactly 12 bytes.")
		val buf = ByteBuffer.wrap(b)
		new ObjectID(buf.getInt(), buf.getInt(), buf.getInt(), false)
	}

	def apply(s: String) = {
		require(ObjectID.isValid(s), "Invalid ObjectID String [%s]".format(s))
		val bytes = new Array[Byte](12)
		for (i <- 0 until 12) bytes(i) = s.substring(i*2, i*2 + 2).toInt
		val buf = ByteBuffer.wrap(bytes)
		new ObjectID(buf.getInt(), buf.getInt(), buf.getInt(), false)
	}
	
	private val increment = new AtomicInteger(new java.util.Random().nextInt())

	/** 
	 * We need a generated machine identifier that doesn't clash.
	 * Copied, for now, from the MongoDB Java Driver
	 */
	lazy val generatedMachineID = {
		// Generate a 2-byte machine piece based on NIC Info
		val machinePiece = {
			import scala.util.JavaConversions._
			try {
				val nics = for (nic <- NetworkInterface.getNetworkInterfaces()) yield nic.toString()
				nics.mkString.hashCode << 16
			} catch {
				case t: Throwable =>
					// Some versions of the IBM JDK May throw exceptions, make a random number
					log.warn("Failed to generate NIC section for ObjectID: " + t.getMessage, t)
					new java.util.Random().nextInt << 16
			}
		}
		log.trace("Generated ObjectID Machine Piece: %s", machinePiece)

		/**
		 * We need a 2 byte process piece. It must represent not only the JVM
		 * but any individual classloader to avoid collisions
		 */
		val processPiece = { 
			val pid = try {
				java.lang.management.ManagementFactory.getRuntimeMXBean.getName.hashCode
			} catch { 
				case t => 
					// fallback
					new java.util.Random().nextInt() 
			}

			val loader = this.getClass.getClassLoader
			val loaderID = if (loader != null) System.identityHashCode(loader) else 0
			(processID.toHexString + loaderID.toHexString).hashCode & 0xFFFF
		}
		log.trace("Generated ObjectID Process Piece: %s", processPiece)

		log.trace("Generated Machine ID for ObjectID: %s", (machinePiece | processPiece).toHexString)
		machinePiece | processPiece
	}

	def nextIncrement() = increment.getAndIncrement()

	/** 
	 * Validates if a string could be a valid <code>ObjectID</code>
	 * @return where or not the string *could* be an ObjectID
	 */
	def isValid(s: String): Boolean = {
		if (s == null) false

		val len = s.length
		if (len != 24) false 
		else { 
			for (i <- 0 until len) {
				val c = s.charAt(i)			
				if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'))
					false
			}

			true

		}
	}
}