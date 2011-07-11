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
import scala.annotation.tailrec
import scala.collection.Set

/**
 * Supports breaking down the MongoDB URI Format for connections.
 *
 * TODO - Make me a more scalaey functional codebase.  Mostly eyeball
 * ported from Java.
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @see http://www.mongodb.org/display/DOCS/Connections
 */
object MongoURI extends Logging {
  final val UriPrefix = "mongodb://"
  final val ConnSpec = "http://www.mongodb.org/display/DOCS/Connections"

  def unapply(uri: String) = {
    val obj = new MongoURI(uri)
    Some(obj.hosts, obj.db, obj.collection, obj.username, obj.password, obj.options)
  }

  protected[mongodb] def parseHosts(svr: String) = {
    val idx = svr.indexOf("@")

    val (s, username, password) = if (idx > 0) {
      val auth = svr.substring(0, idx)

      val ai = auth.indexOf(":")
      (svr.substring(idx + 1),
        Some(auth.substring(0, ai)),
        Some(auth.substring(ai + 1)))
    } else (svr, None, None)

    def _createHost(data: Array[String]) = if (data.length > 1)
      (data(0), data(1).toInt)
    else (data(0), 27017)

    (s.split(",").toSet.map { x: String => _createHost(x.split(":")) },
      username, password)
  }

}

class MongoURI(uri: String) extends Logging {
  import MongoURI._

  require(uri.startsWith(UriPrefix),
    "MongoDB URIs must start with %s. See: %s".format(UriPrefix, ConnSpec))
  val _uri = uri.substring(UriPrefix.length)

  val idx = _uri.lastIndexOf("/")
  val (svr, ns, opts) = if (idx < 0) (_uri, None, None) else {
    val s = _uri.substring(0, idx)
    val n = _uri.substring(idx + 1)

    val qi = n.indexOf("?")
    if (qi >= 0) {
      val o = n.substring(qi + 1)
      val _n = n.substring(0, qi)
      (s, Some(_n), Some(o))
    } else (s, Some(n), None)
  }

  log.info("Server: %s, NS: %s, Options: %s", svr, ns, opts)

  val (_hosts, _username, _password) = parseHosts(svr)

  log.info("Hosts: %s, Username: %s, Password: %s", hosts, username, password)

  val (_db, _coll) = ns match {
    case Some(_ns) => {
      val ni = _ns.indexOf(".")
      if (ni < 0) (Some(_ns), None) else (Some(_ns.substring(0, ni)), Some(_ns.substring(ni + 1)))
    }
    case None => (None, None)
  }

  log.info("DB: %s Coll: %s", _db, _coll)

  opts.foreach {
    _.split("&|;").foreach { o =>
      val _i = o.indexOf("=")
      if (_i >= 0) {
        val key = o.substring(0, _i).toLowerCase
        val value = o.substring(_i + 1)
        key match {
          case "maxpoolsize" => options.maxPoolSize = value.toInt
          case "minpoolsize" => options.minPoolSize = value.toInt
          case "waitqueuemultiple" => options.waitQueueMultiple = value.toInt
          case "waitqueuetimeoutms" => options.waitQueueTimeout = value.toInt
          case "connecttimeoutms" => options.connectTimeoutMS = value.toInt
          case "sockettimeoutms" => options.socketTimeoutMS = value.toInt
          case "autoconnectretry" => options.autoConnectRetry = _parseBool(value)
          case "socketkeepalive" => options.socketKeepAlive = _parseBool(value)
          case "slaveok" => options.slaveOK = _parseBool(value)
          case "safe" => options.safe = _parseBool(value)
          case "w" => options.w = value.toInt
          case "wtimeout" => options.wtimeout = value.toInt
          case "fsync" => options.fsync = _parseBool(value)
          case unknown => log.warn("Unknown or unsupported option '%s'", value)
        }
      }
    }
  }

  protected[mongodb] def _parseBool(_in: String) = {
    val in = _in.trim
    if (!in.isEmpty && in == "1" ||
      in.toLowerCase == "true" || in.toLowerCase == "yes")
      true
    else
      false
  }

  val options = MongoOptions()

  def hosts = _hosts

  def username = _username

  def password = _password

  def db = _db

  def collection = _coll
}

case class MongoOptions(
  var slaveOK: Boolean = false, /* Read from secondaries permitted */
  var safe: Boolean = false, /* getLastError after every update */
  var w: Int = 0, /* w:n added to getLastError; implies safe=true */
  var wtimeout: Int = 0, /* MS for getLastError timeouts */
  var fsync: Boolean = false, /* adds fsync:true to the getLastError cmd */
  var journal: Boolean = false, /* Sync to journal; implies safe=true */
  var connectTimeoutMS: Int = 0, /* How long a conn can take to be opened before timing out */
  var socketTimeoutMS: Int = 0, /* How long a send or receive on a socket can take before timing out */
  var socketKeepAlive: Boolean = false,
  var autoConnectRetry: Boolean = false,
  var waitQueueTimeout: Int = 1000 * 60 * 2,
  var waitQueueMultiple: Int = 5,
  var maxPoolSize: Int = 10,
  var minPoolSize: Int = 1)

// vim: set ts=2 sw=2 sts=2 et:
