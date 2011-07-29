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
import akka.actor.Actor
import akka.actor.ActorRef
import java.util.concurrent.atomic.AtomicBoolean
import akka.dispatch.Future

/**
 * A single socket open to a single MongoDB instance. Normally you would
 * want to use a connection pool, rather than this class. Construct
 * a connection pool with MongoConnection(address). Then get a direct
 * connection with "connection.direct"
 */
class DirectConnection(val addr: InetSocketAddress,
                       override protected val connectionActor: ActorRef,
                       initiallyConnected: Boolean,
                       initiallyIsMaster: Boolean,
                       initialMaxBSONObjectSize: Int)
    extends MongoConnection with Logging {

  log.info("Initializing Direct MongoDB connection on address '%s'", addr)

  private val _isMaster = new AtomicBoolean(initiallyIsMaster)
  private val _connected = new AtomicBoolean(initiallyConnected)

  override def connected_? = _connected.get

  override def isMaster = _isMaster.get

  /**
   * Checks the max object size on the wire for this connection.
   * FIXME does this ever change post-connect? can it just be constant? If it
   * does change, it can change at any time so using it may create races.
   * For now we'll just leave it permanently with the initial value
   * retrieved at connect time. If it could change we'd handle it
   * the same way as isMaster
   */
  val maxBSONObjectSize = initialMaxBSONObjectSize

  private case class CheckMasterState(isMaster: Boolean, maxBSONObjectSize: Int)
  private var checkMasterState: Option[CheckMasterState] = None

  // Called by ConnectionChannelActor to maintain our state.
  // this is sort of a hack, but since ConnectionChannelActor caches
  // the DirectConnection anyway, it seems dumb to pass this state
  // via messages, when the actor already has a reference to us
  // and can just set the state.
  protected[mongodb] def setState(state: ConnectionChannelActor.State) = {
    _isMaster.set(state.isMaster)
    _connected.set(state.connected)
  }

  override def direct: DirectConnection = this
}
