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
import java.nio.ByteOrder
import java.net.InetSocketAddress
import akka.dispatch.Dispatchers
import akka.dispatch.Future

protected[mongodb] class ConnectionPoolActor(private val addr: InetSocketAddress)
  extends ConnectionActor
  with Actor
  with DefaultActorPool
  with BoundedCapacityStrategy
  with MailboxPressureCapacitor // overrides pressureThreshold based on mailboxes
  with SmallestMailboxSelector
  with Filter
  with RunningMeanBackoff
  with BasicRampup {

  override def receive = {
    case m if _route.isDefinedAt(m) => {
      log.trace("pool %s routing message %s",
        self.uuid, m)
      _route.apply(m)
    }
  }

  // BoundedCapacitor min and max actors in pool. No real rationale for the
  // upper bound here. should probably be configurable.
  override val lowerBound = 1
  override val upperBound = 1 // FIXME

  // this stuff is all just random for now
  override val pressureThreshold = 1
  override val partialFill = true
  override val selectionCount = 1
  override val rampupRate = 0.1
  override val backoffRate = 0.50
  override val backoffThreshold = 0.50

  // this dispatcher is work-stealing, so if a connection is stuck others can take its work;
  // it also has a thread pool with core pool size of upperBound, which means we
  // won't create threads until upperBound threads are used up.
  // The thread pool queue has fixed capacity of 1 because otherwise we'd never create
  // threads above core pool size even if all threads were busy.
  private val childDispatcher =
    Dispatchers.newExecutorBasedEventDrivenWorkStealingDispatcher("Hammersmith Connection Dispatcher").
      withNewThreadPoolWithLinkedBlockingQueueWithCapacity(1).
      setCorePoolSize(upperBound).
      buildThreadPool

  override def instance = {
    val actorRef = Actor.actorOf(new ConnectionChannelActor(addr))
    //actorRef.dispatcher = childDispatcher
    log.trace("ConnectionPoolActor %s created new pooled instance %s", self.uuid, actorRef.uuid)
    actorRef
  }
}
