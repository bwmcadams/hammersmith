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

import org.bson.util.Logging

import java.net.InetSocketAddress
import org.jboss.netty.bootstrap.ClientBootstrap
/**
 * Direct Connection to a single mongod (or mongos) instance
 *
 * NOTE: Connection instances are instances of a *POOL*, always.
 */
class DirectConnection(val addr: InetSocketAddress) extends MongoConnection with Logging {

  log.info("Initialized Direct MongoDB connection on address '%s'", addr)

  lazy val handler = new DirectConnectionHandler(bootstrap)

}

protected[mongodb] class DirectConnectionHandler(val bootstrap: ClientBootstrap) extends MongoConnectionHandler
