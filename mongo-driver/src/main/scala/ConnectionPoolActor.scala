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
import java.net.InetSocketAddress
import akka.dispatch.Dispatchers
import akka.dispatch.Future

protected[mongodb] class ConnectionPoolActor(private val addr: InetSocketAddress)
  extends ConnectionActor
  with Actor
  with DefaultActorPool
  with BoundedCapacityStrategy
  // there's an NPE in Akka with this capacitor right now.
  //with MailboxPressureCapacitor // overrides pressureThreshold based on mailboxes
  with ActiveFuturesPressureCapacitor // mailbox makes way more sense, but this isn't broken for now
  with SmallestMailboxSelector
  //with RunningMeanBackoff
  // With a backoff filter, ActorPool kills connections immediately
  // even with pending replies, while we need to first stop sending them
  // stuff and then leave them for a timeout, I guess.
  // FIXME For now, just never stop connections.
  with BasicNoBackoffFilter
  with BasicRampup {

  override def receive = _route

  // BoundedCapacitor min and max actors in pool.
  // should probably be configurable.
  override val lowerBound = 1
  override val upperBound = Runtime.getRuntime.availableProcessors * 2

  // this stuff is all just random for now
  // should probably be configurable somehow or in some respects.
  // FIXME pressureThreshold goes with MailboxPressureCapacitor which is disabled due to Akka NPE bug
  //override val pressureThreshold = 1
  override val partialFill = true
  override val selectionCount = 1
  override val rampupRate = 0.1
  // FIXME disabled along with RunningMeanBackoff
  //override val backoffRate = 0.50
  //override val backoffThreshold = 0.50

  // this is our dispatcher and we also pass it to all child actors
  // and we use it when creating a cursor actor
  self.dispatcher =
    // do not work-steal because we'd take messages from one netty channel and give
    // them to the wrong actor
    Dispatchers.newExecutorBasedEventDrivenDispatcher("Hammersmith Connection Dispatcher").
      // synchronous queue because a queue that can hold items keeps us from going above CorePoolSize,
      // which leads to deadlocks. this can't be set in akka.conf, unfortunately
      withNewThreadPoolWithSynchronousQueueWithFairness(false).
      // normally we want a thread per connection in the pool
      setCorePoolSize(upperBound).
      // unbounded max threads, or we could get deadlocks
      setMaxPoolSize(Int.MaxValue).
      build

  override def instance = {
    val actorRef = Actor.actorOf(new ConnectionChannelActor(addr))
    log.trace("ConnectionPoolActor %s created new pooled instance %s", self.uuid, actorRef.uuid)
    actorRef.dispatcher = self.dispatcher
    actorRef
  }
}
