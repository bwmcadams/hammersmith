/*
 * Copyright (c) 2011-2016 Brendan McAdams <http://bytes.codes>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package codes.bytes.hammersmith.bson.primitive

import scala.util.Try

/**
  * Mongo Java Driver always works with seconds, not milliseconds...
  * @param time
  * @param increment
  */
final case class MongoTimestamp(
  time: Option[java.util.Date] = None,
  increment: Int = 0
  ) extends Ordered[MongoTimestamp] {

  def this(time: Int, inc: Int) = this(Some(new java.util.Date(time * 1000)), inc)

  def this(pair: (Int, Int)) = this(pair._1, pair._2)

  def epoch: Int = Try(epoch / 1000).getOrElse(0)

  // based on Mongo Java Driver's compare
  override def compare(that: MongoTimestamp): Int = {
    if (this.epoch != that.epoch)
      this.epoch - that.epoch
    else
      this.increment - that.increment
  }


  // based on Mongo Java's
  override def hashCode(): Int = {
    val prime = 31
    prime * (prime * 1 + increment) + epoch
  }


  // based on Mongo Java's
  override def equals(that: scala.Any): Boolean = that match {
    case other: MongoTimestamp =>
    this.epoch == other.epoch && this.increment == other.increment


  }

}

// vim: set ts=2 sw=2 sts=2 et:
