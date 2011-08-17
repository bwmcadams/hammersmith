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
import replication.{ReplicaSetArbiter , PassiveReplicaSetMember , ActiveReplicaSetMember}
import org.bson.collection.Document
import com.mongodb.ReplicaSetStatus

/**
* "IS Master" Status
*/
trait MongodStatus {
  def isMaster: Boolean
  def maxBSONObjectSize: Int

}

case object DisconnectedServerStatus extends MongodStatus {
  val isMaster = false
  val maxBSONObjectSize = MongoMessage.DefaultMaxBSONObjectSize
}

case class SingleServerStatus(val isMaster: Boolean, val maxBSONObjectSize: Int) extends MongodStatus

class ReplicaSetMemberStatus(val isMaster: Boolean,
  val maxBSONObjectSize: Int,
  val setName: String,
  val primary: ActiveReplicaSetMember,
  val isSecondary: Boolean,
  val hosts: Iterable[ActiveReplicaSetMember],
  val passives: Iterable[PassiveReplicaSetMember],
  val arbiters: Iterable[ReplicaSetArbiter]) extends MongodStatus

object ReplicaSetMemberStatus {

  def apply(isMaster: Boolean,
            maxBSONObjectSize: Int,
            setName: String,
            primary: ActiveReplicaSetMember,
            isSecondary: Boolean,
            hosts: Iterable[ActiveReplicaSetMember],
            passives: Iterable[PassiveReplicaSetMember],
            arbiters: Iterable[ReplicaSetArbiter]) = new ReplicaSetMemberStatus(isMaster, maxBSONObjectSize, setName, primary,
                                                                                isSecondary, hosts, passives, arbiters)


  def unapply(doc: Option[String]) = {
    val (isMaster, maxBSON) = MongodStatus.parseBasicIsMaster(doc)
    val setName = doc.getAs[String]("setName")
    val isSecondary = doc.getAs[Boolean]("secondary")
    val primary = if (isSecondary.get) {
      doc.getAs[String]("primary") match {
        case Some(addr: String) ⇒ ActiveReplicaSetMember.tupled(parseAddress(addr))
        case None ⇒ throw new IllegalStateException("I am a Secondary, but no Primary hostname reported.  Replica Set may be unhealthy.")
      }
    } else {
      // Set the primary to ourselves if we are primary; it isn't reported by mongo in that case
      require(maybeIsMaster.isDefined && maybeIsMaster.get, "Server is reported as neither a secondary or a primary. Logical consistency error.")
      ActiveReplicaSetMember.tupled(MongodStatus.parseHostAddress(addressString.split("/").last))
    }

    // Attempt to parse out the active list
    val hosts = doc.getAsOrElse[Seq[String]]("hosts", Seq.empty[String]) map { host ⇒ ActiveReplicaSetMember.tupled(parseAddress(host)) }

    // Attempt to parse out the passives list
    val passives = doc.getAsOrElse[Seq[String]]("passives", Seq.empty[String]) map { host ⇒ PassiveReplicaSetMember.tupled(parseAddress(host)) }

    // Attempt to parse out the arbiter list
    // TODO - Should we get this? Can we use it for anything useful such as election monitoring?
    val arbiters = doc.getAsOrElse[Seq[String]]("arbiters", Seq.empty[String]) map { host ⇒ ReplicaSetArbiter.tupled(parseAddress(host)) }

    ReplicaMemberStatus(isMaster, maxBSON, setName, primary, isSecondary, hosts, passives, arbiters)
  }

}

object MongodStatus {
  protected[mongodb] def parseHostAddress(addr: String) = {
    val arr = addr.split(":")
    if (arr.length == 1)
      (arr(0), 27017) // Newer version of mongo should of course always return port
    else if (arr.length == 2)
      (arr(0), arr(1).toInt)
    else
      throw new IllegalArgumentException("Invalid/unparseable host address '%s'".format(addr))
  }

  protected[mongodb] def parseBasicIsMaster(doc: Document) = {
    val isMaster = doc.getAs[Boolean]("ismaster") match {
      case Some(m: Boolean) => m
      case Some(other) =>  throw new IllegalArgumentException("Invalid value set for 'ismaster' (%s); cannot proceed".format(other))
      case None => throw new IllegalStateException("No value set for 'ismaster'; cannot proceed.")
    }

    val maxBSON = doc.getAsOrElse[Int]("maxBsonObjectSize", MongoMessage.DefaultMaxBSONObjectSize)

    log.trace("isMaster reply was isMaster=%s bson object size %s", isMaster, maxBSON)
    (isMaster, maxBSON)
  }

  def apply(doc: Document) = {
    log.debug("Decoding isMaster result (%s)", doc)


    // Are we in a Replica Set? If so, parse out (TODO - does 1.6 support setName or do we just fail to support 1.6.x?)
    doc.getAs[String]("setName") match {
      case ReplicaSetMemberStatus(rsStatus) =>
        log.info("Returning a Replica Set Member Status { %s }", rsStatus)
        rsStatus
      case None =>
        log.debug("Not running in a Replica Set; no 'setName' field present.")
        SingleServerStatus.tupled(parseBasicIsMaster(doc))

    }


  }

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
