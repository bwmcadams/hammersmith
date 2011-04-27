/**
 * Copyright (c) 2010, 2011 10gen, Inc. <http://10gen.com>
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

package com.mongodb.async
package util

import com.twitter.conversions.time._
import scala.collection.mutable.HashSet
import scala.ref.WeakReference
import com.twitter.util._
import org.bson.util.Logging

/**
 * Based on com.twitter.util.ReferenceCountedTimer
 * Tracks if any active channels are held and turns on / off the cleaner as needed
 */
protected[mongodb] class CursorCleaningTimer(val period: Duration = 5.seconds) extends Logging {
  private[this] val connections = HashSet.empty[WeakReference[MongoConnection]]
  private[this] var underlying: Timer = null
  private[this] val factory = () => new JavaTimer(true)

  def acquire(conn: MongoConnection) = synchronized {
    connections += new WeakReference(conn)
    if (!connections.isEmpty) {
      if (underlying == null) {
        underlying = factory()
        scheduleCleanup()
      }
    }
  }

  def stop(conn: MongoConnection) = synchronized {
    connections -= new WeakReference(conn)
    if (connections.isEmpty) {
      log.info("Connections empty.  Stopping scheduler thread.")
      underlying.stop()
      underlying = null
    }
  }

  protected[mongodb] def scheduleCleanup() =  underlying.schedule(period.fromNow, period) {
    MongoConnection.cleanup()
  }

}

