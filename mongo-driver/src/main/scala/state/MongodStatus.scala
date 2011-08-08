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
package state

import wire.MongoMessage
import org.jboss.netty.channel.Channel

/**
 * "IS Master" Status
 */
trait MongodStatus {
  def isMaster: Boolean
  def maxBSONObjectSize: Int
  def channel: Option[Channel]
}

case object DisconnectedServerStatus extends MongodStatus {
  val isMaster = false
  val maxBSONObjectSize = MongoMessage.DefaultMaxBSONObjectSize
  val channel = None
}

case class SingleServerStatus(val isMaster: Boolean, val maxBSONObjectSize: Int,
                              val channel: Option[Channel]) extends MongodStatus

