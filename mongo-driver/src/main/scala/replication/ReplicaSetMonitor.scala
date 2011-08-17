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

import akka.actor.Actor
import akka.actor.Actor._
import akka.routing.Dispatcher
import org.bson.util.Logging
import java.net.InetSocketAddress

/**
 * Supervised actor chain responsible for discovering and monitoring
 * Replica Set topography and reporting it to the connection pool / routing systems
 * to setup correct connections, handle failovers, etc.
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @since 0.5
 *
 * TODO - Pull Replica Set Config for tags, priority, slave delay, etc
 *
 * TODO - Support polling replication lag
 */
abstract class ReplicaSetMonitor(private val replSetName: String,
                                 private val seedAddresses: Seq[InetSocketAddress]) extends Actor with Logging {

  final val UnauthenticatedErrorCode = 10057

  val id = "ReplicaSetMonitor.%s".format(replSetName)

  override def preStart() {
    log.info("Starting up %s", id)
  }

  override def postStop() {
    log.info("Stopped %s", id)
  }

  override def postRestart(reason: Throwable) {
    log.warn("Restarted %s. Reason: '%s'", id, reason.getMessage)
  }

  override def preRestart(reason: Throwable) {
    log.warn("Restarting %s. Reason: '%s'", id, reason.getMessage)
  }

}
