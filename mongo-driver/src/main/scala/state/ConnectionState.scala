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

/**
 *  Basics of a connection state.
 *  Are we connected
 */
sealed trait ConnectionState {
  /**
   * Simple Channel State: Did we open a connection to the server?
   */
  def isChannelEstablished: Boolean

  /**
   * Negotiated means we connected the channel and negotiated it (e.g. gotIsMaster)
   */
  def isChannelNegotiated: Boolean

  def isConnected: Boolean

  /**
   * Shutdown of this connection is presently in progress,
   * and special rules apply
   * TODO - Document Quiescing Behavior
   */
  def isQuiescing: Boolean
}

case object EstablishingChannel extends ConnectionState {
  val isChannelEstablished = false
  val isChannelNegotiated = false
  val isConnected = false
  val isQuiescing = false
}

case object NegotiatingChannel extends ConnectionState {
  val isChannelEstablished = true
  val isChannelNegotiated = false
  val isConnected = false
  val isQuiescing = false
}

case object Connected extends ConnectionState {
  val isChannelEstablished = true
  val isChannelNegotiated = true
  val isConnected = true
  val isQuiescing = false
}

case object Quiescing extends ConnectionState {
  val isChannelEstablished = true
  val isChannelNegotiated = true
  val isConnected = true
  val isQuiescing = true
}

case object Disconnected extends ConnectionState {
  val isChannelEstablished = false
  val isChannelNegotiated = false
  val isConnected = false
  val isQuiescing = false
}
