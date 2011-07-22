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

import akka.actor.{ Channel => AkkaChannel, _ }
import akka.routing._
import com.mongodb.async.wire._
import org.jboss.netty.channel._
import org.jboss.netty.buffer._
import org.bson.util.Logging
import java.nio.ByteOrder
import java.net.InetSocketAddress
import akka.dispatch.Future

protected[mongodb] class ConnectionPoolActor(val addr: InetSocketAddress)
  extends ConnectionActor
  with Logging
  with Actor
  with DefaultActorPool
  with BoundedCapacityStrategy
  with MailboxPressureCapacitor // overrides pressureThreshold based on mailboxes
  with SmallestMailboxSelector
  with Filter
  with RunningMeanBackoff
  with BasicRampup {

  override def receive = _route

  // BoundedCapacitor min and max actors in pool. No real rationale for the
  // upper bound here. should probably be configurable.
  override val lowerBound = 1
  override val upperBound = 10

  // this stuff is all just random for now
  override val pressureThreshold = 1
  override val partialFill = true
  override val selectionCount = 1
  override val rampupRate = 0.1
  override val backoffRate = 0.50
  override val backoffThreshold = 0.50

  /*
   *  The theory here is to always keep a
   *  future-netty-channel pending (once on pool creation, and then up to the max
   *  pool size, it always starts opening a new channel whenever a new actor is created).
   *  There's no way in Akka to do ActorPool.instance() asynchronously, so when the pool
   *  goes to create a new actor we'll just block until the channel opens. But, since
   *  we start opening a new channel earlier, we hopefully won't block too long.
   *  The alternative is to make each individual single-channel actor able to work
   *  without the channel, and that's kind of a nightmare.
   *  Two Akka fixes that would help here would be:
   *   - allow an async instance()
   *   - allow the single-channel actors to avoid receiving any messages until the
   *     channel is established, like a self.suspend()/self.unsuspend() kind of API
   *  Better ideas welcome.
   */
  private case class ReadyChannelInfo(channel: Channel, maxBSONObjectSize: Int)
  private var nextChannel: Future[ReadyChannelInfo] = null /* FIXME move channel-creation code over here */

  override def instance = {
    val thisChannel = nextChannel.get // we just block FIXME what to do if channel creation fails?
    nextChannel = null // FIXME start loading another channel, unless we already have upperBound
    Actor.actorOf(new DirectConnectionActor(thisChannel.channel, thisChannel.maxBSONObjectSize))
  }
}
