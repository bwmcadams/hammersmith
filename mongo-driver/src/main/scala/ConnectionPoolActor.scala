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

protected[mongodb] class ConnectionPoolActor(private val addr: InetSocketAddress)
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

  override def instance = {
    Actor.actorOf(new ConnectionChannelActor(addr))
  }
}
