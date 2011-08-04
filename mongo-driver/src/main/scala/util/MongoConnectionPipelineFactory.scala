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
package util

import org.bson.util.Logging
import akka.actor.ActorRef
import wire.ReplyMessageDecoder
import org.jboss.netty.channel.{ Channels, ChannelPipelineFactory }
import com.mongodb.async.ConnectionChannelActor.{ ConnectionSetupHandler, ConnectionActorHandler }
import java.util.concurrent.atomic.AtomicBoolean

abstract class MongoConnectionPipelineFactory extends ChannelPipelineFactory with Logging {
  def connectionActor: ActorRef
  def addressString: String
  protected def setupHandler: ConnectionSetupHandler
  protected def actorHandler: Option[ConnectionActorHandler]

  protected lazy val pipeline = Channels.pipeline(new ReplyMessageDecoder(), setupHandler)
  override def getPipeline = pipeline

  protected var setup = new AtomicBoolean(false)

  def awaitSetup() {
    assume(!setup.get())
    setupHandler.await()
    /**
     * If one is defined, swap in the real handler
     * In some cases such as Replica Set monitoring we never want to
     * swap out.
     */
    log.info("Swapping out Pipeline with ActorHandler: %s", actorHandler)
    actorHandler.foreach(pipeline.replace(setupHandler, "actorHandler", _))
    setup.set(true)
  }

  // can only call these after awaitSetup
  def setupFailed = setupHandler.failed
  def setupFailure = setupHandler.failure
  def isMaster = setupHandler.isMaster
  def maxBSONObjectSize = setupHandler.maxBSONObjectSize

}