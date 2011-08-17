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
package replication

import akka.actor.Actor._
import akka.routing.{ Dispatcher, InfiniteIterator }
import akka.actor.{ ActorRef, Actor }
import java.net.InetSocketAddress
import org.bson.util.Logging

/**
 * "Core" Message Dispatcher for Hammersmith which is aware of and monitors replication.
 * It also manages the balancing and routing of messages based on
 *
 *
 * TODO - We should be using supervisors to manage all the pools, etc
 */
abstract class ReplicationAwareMessageDispatcher(private val replSetName: String,
                                                 private val seedAddresses: Seq[InetSocketAddress]) extends Actor with Dispatcher with Logging {
  protected def primary: ActorRef // The pool for the primary (or master, or single mongod)
  protected def secondaries: InfiniteIterator[ActorRef] // All read-only members: secondaries or slaves

  override def broadcast(message: Any) = throw new UnsupportedOperationException("Broadcast Operations are not "
    + "supported with MongoDB.")

  override def preStart() {
    log.info("Starting up ReplicationAwareMessageDispatcher.")

  }

  override def postStop() {
    log.info("Stopped ReplicationAwareMessageDispatcher.")
  }

  override def postRestart(reason: Throwable) {
    log.warn("Restarted ReplicationAwareMessageDispatcher. Reason: '%s'", reason.getMessage)
  }
  override def preRestart(reason: Throwable) {
    log.warn("Restarting ReplicationAwareMessageDispatcher. Reason: '%s'", reason.getMessage)
  }
}
