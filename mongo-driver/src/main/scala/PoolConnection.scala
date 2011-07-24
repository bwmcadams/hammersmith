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

import org.bson.util.Logging
import java.net.InetSocketAddress
import org.jboss.netty.bootstrap.ClientBootstrap
import akka.actor.Actor
import akka.actor.ActorRef

/**
 * A pool of sockets open to a single MongoDB instance. Construct
 * a connection pool with MongoConnection(address) rather than
 * directly.
 */
class PoolConnection(val addr: InetSocketAddress, override protected val connectionActor: ActorRef) extends MongoConnection with Logging {

  log.info("Initializing Pool MongoDB connection on address '%s'", addr)

  def this(addr: InetSocketAddress) = {
    this(addr, Actor.actorOf(new ConnectionPoolActor(addr)).start)
  }

  override def direct: DirectConnection = {
    (connectionActor !! ConnectionActor.GetDirect) match {
      case Some(reply: ConnectionActor.GetDirectReply) =>
        reply.direct
      case _ =>
        throw new Exception("Failed to retrieve a direct connection; timeout, or none open? not sure")
    }
  }

  // for now, define "connected" to mean "an arbitrary actor in the pool is connected"
  override def connected_? = direct.connected_?
  // for now, define "isMaster" to mean "an arbitrary actor in the pool is master"
  override def isMaster = direct.isMaster
}
