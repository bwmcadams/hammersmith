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
package com.mongodb.async.replication

import java.net.InetSocketAddress
import com.mongodb.async.util.MongoConnectionPipelineFactory
import com.mongodb.async.{ ConnectionActor, ConnectionChannelActor }
import org.jboss.netty.channel.{ ChannelFuture, ChannelFutureListener }
import akka.actor.ActorRef
import com.mongodb.async.ConnectionChannelActor.{ ConnectionSetupHandler, ConnectionActorHandler }
import org.bson.SerializableBSONObject
import org.bson.collection.Document
import com.mongodb.async.wire.{ MongoMessage, ReplyMessage }
import java.lang.IllegalStateException
import com.mongodb.async.state.MongodStatus



private[mongodb] class ReplicaMemberMonitorActor(memberAddr: InetSocketAddress) extends ConnectionChannelActor(memberAddr) {
  import ConnectionActor._
  import ConnectionChannelActor._

  log.trace("Constructing ReplicaMemberMonitorActor to monitor member '%s'", memberAddr.toString)

  self.id = "ReplicaMemberMonitorActor(memberAddr=%s)".format(addr.toString)

  override lazy val pipelineFactory = new ReplicaMonitorPipelineFactory(self, addressString)

  /*  override protected def setupCompleted(pipelineFactory: MongoConnectionPipelineFactory): Unit = pipelineFactory match {
    case replicaPipeline: ReplicaMonitorPipelineFactory ?
      maxBSONObjectSize = pipelineFactory.maxBSONObjectSize
      isMaster = pipelineFactory.isMaster
      setName = pipelineFactory.setName
      primary = pipelineFactory.primary
      isSecondary = pipelineFactory.isSecondary
      hosts = pipelineFactory.hosts
      passives = pipelineFactory.passives
      arbiters = pipelineFactory.arbiters

      log.debug("Resuming %s %s with max size %d and isMaster %s",
        addressString, self.uuid, maxBSONObjectSize, isMaster)
    case other ?
      log.warn("Unknown Pipeline type '%s'; expected a ReplicaMonitor PipelineFactory", other.getClass)
  }*/

  protected var setName: Option[String] = None
  protected var primary: Option[ActiveReplicaSetMember] = None
  protected var isSecondary: Option[Boolean] = None
  // "Active" hosts
  protected var hosts: Option[Seq[ActiveReplicaSetMember]] = None
  // "Passive" hosts
  protected var passives: Option[Seq[PassiveReplicaSetMember]] = None
  // Arbiters
  protected var arbiters: Option[Seq[ReplicaSetArbiter]] = None

}

/* Pipeline factory generates a pipeline with our decoder and handler */
class ReplicaMonitorPipelineFactory(val connectionActor: ActorRef,
                                    val addressString: String) extends MongoConnectionPipelineFactory {

  val actorHandler = None
  val setupHandler = new ReplicaMonitorSetupHandler(addressString)

  def setName = setupHandler.setName
  def primary = setupHandler.primary
  def isSecondary = setupHandler.isSecondary
  def hosts = setupHandler.hosts
  def passives = setupHandler.passives
  def arbiters = setupHandler.arbiters

}

sealed trait ReplicaSetMember {
  def host: String
  def port: Int
}

case class ActiveReplicaSetMember(val host: String, val port: Int) extends ReplicaSetMember
case class PassiveReplicaSetMember(val host: String, val port: Int) extends ReplicaSetMember
case class ReplicaSetArbiter(val host: String, val port: Int) extends ReplicaSetMember

class ReplicaMonitorSetupHandler(addressString: String) extends ConnectionSetupHandler(addressString) {

  protected var _setName: Option[String] = None
  protected var _primary: Option[ActiveReplicaSetMember] = None
  protected var _isSecondary: Option[Boolean] = None
  // "Active" hosts
  protected var _hosts: Option[Seq[ActiveReplicaSetMember]] = None
  // "Passive" hosts
  protected var _passives: Option[Seq[PassiveReplicaSetMember]] = None
  // Arbiters
  protected var _arbiters: Option[Seq[ReplicaSetArbiter]] = None

  def setName = _setName
  def primary = _primary
  def isSecondary = _isSecondary
  def hosts = _hosts
  def passives = _passives
  def arbiters = _arbiters



  /**
   * TODO - Make this method easily unit testable
   */
  override protected[mongodb] def processIsMasterReply(reply: ReplyMessage) {
    /**
     *  It isn't our job to validate any info such as whether we're on a Replica Set,
     *  what the name is etc.  We merely pass it up the stack to those who care.
     *
     *  If we don't get values for expected Repl Values, we simply return them as None and let someone
     *  else decide how to behave.
     *
     *  Why? We may actually simply be helping determine if there *IS* a replica set.
     */
    val doc = implicitly[SerializableBSONObject[Document]].decode(reply.documents.head)
    log.debug("Got a result from isMaster command: %s", doc)
    // *REQUIRE* this to be set. If it isn't throw a hard exception
    maybeIsMaster = doc.getAs[Boolean]("ismaster") match {
      case Some(m) ⇒ Some(m)
      case None ⇒ throw new IllegalStateException("No value set for 'ismaster'; cannot proceed.")
    }

    // Only reported by MongoDB 1.7.5+
    maybeMaxBSONObjectSize = Some(doc.getAsOrElse[Int]("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize))

    log.trace("isMaster reply was isMaster=%s bson object size %s", maybeIsMaster.get, maybeMaxBSONObjectSize.get)
    /**
     * these are the bare minimum. Whether replica set or not, these MUST be defined at the least or something is
     *  wrong.
     */
    require(maybeMaxBSONObjectSize.isDefined &&
      maybeIsMaster.isDefined &&
      connected)

    // Parse out Replica Set Properties if the previous went OK
    _setName = doc.getAs[String]("setName")
    _isSecondary = doc.getAs[Boolean]("secondary")
    _primary = if (_isSecondary.get) {
      doc.getAs[String]("primary") match {
        case Some(addr: String) ⇒ Some(ActiveReplicaSetMember.tupled(parseAddress(addr)))
        case None ⇒ throw new IllegalStateException("I am a Secondary, but no Primary hostname reported.  Replica "
          + "Set may be unhealthy.")
      }
    } else {
      // Set the primary to ourselves if we are primary; it isn't reported by mongo in that case
      require(maybeIsMaster.isDefined && maybeIsMaster.get,
        "Server is reported as neither a secondary or a primary. Logical consistency error.")
      Some(ActiveReplicaSetMember.tupled(parseAddress(addressString.split("/").last)))
    }

    // Attempt to parse out the active list
    // TODO - should this be an empty list if empty, or None ?
    _hosts = doc.getAsOrElse[Seq[String]]("hosts", Seq.empty[String]) match {
      case Nil ⇒ None
      case entries ⇒ Some(entries map { host ⇒ ActiveReplicaSetMember.tupled(parseAddress(host)) })
    }

    // Attempt to parse out the passives list
    // TODO - should this be an empty list if empty, or None ?
    _passives = doc.getAsOrElse[Seq[String]]("passives", Seq.empty[String]) match {
      case Nil ⇒ None
      case entries ⇒ Some(entries map { host ⇒ PassiveReplicaSetMember.tupled(parseAddress(host)) })
    }

    // Attempt to parse out the arbiter list
    // TODO - should this be an empty list if empty, or None ?
    // TODO - Should we get this? Can we use it for anything useful such as election monitoring?
    _arbiters = doc.getAsOrElse[Seq[String]]("arbiters", Seq.empty[String]) match {
      case Nil ⇒ None
      case entries ⇒ Some(entries map { host ⇒ ReplicaSetArbiter.tupled(parseAddress(host)) })
    }

  }

}
