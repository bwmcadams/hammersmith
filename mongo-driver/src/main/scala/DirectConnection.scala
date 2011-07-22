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
 * Direct Connection to a single mongod (or mongos) instance
 *
 * NOTE: Connection instances are instances of a *POOL*, always.
 * FIXME does this mean DirectConnection is a pool to one mongod, but still a pool?
 */
class DirectConnection(val addr: InetSocketAddress) extends MongoConnection with Logging {

  log.info("Initialized Direct MongoDB connection on address '%s'", addr)

  /*
   *  FIXME channel won't exist here yet. I have a thought on how to do this.
   *  Basically the actor pool subclass of ConnectionActor will always keep a
   *  future-netty-channel pending (once on actor creation, and then up to the max
   *  pool size, it always starts opening a new channel whenever a new actor is created).
   *  There's no way in Akka to do ActorPool.instance() asynchronously, so when the pool
   *  goes to create a new actor we'll just block until the channel opens. But, since
   *  we start opening a new channel earlier, we hopefully won't block too long.
   *  The alternative is to make each individual single-channel actor able to work
   *  without the channel, and that's a nightmare.
   *  Two Akka fixes that would help here would be: allow an async instance(), and/or
   *  allow the single-channel actors to avoid receiving any messages until the
   *  channel is established.
   *  Better ideas welcome.
   */
  override protected val connectionActor = Actor.actorOf(new DirectConnectionActor(channel, maxBSONObjectSize)).start

  lazy val handler = new DirectConnectionHandler(bootstrap, connectionActor)
}

protected[mongodb] class DirectConnectionHandler(override val bootstrap: ClientBootstrap,
  override val connectionActor: ActorRef) extends MongoConnectionHandler {
}
