/**
  * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package codes.bytes.hammersmith.wire

import codes.bytes.hammersmith.collection.immutable.Document

/*
 * Note that no matter what any other settings are this is only called if
 * w > 0
 * @param w (Int) Specifies the number of servers to wait for on the write operation, and exception raising behavior. Defaults to {@code 0}
 * @param wTimeout (Int) Specifies the number MS to wait for the server operations to write.  Defaults to 0 (no timeout)
 * @param fsync (Boolean) Indicates whether write operations should require a sync to disk. Defaults to False
 * @param j (Boolean) *1.9+ ONLY* When true, the getlasterror call awaits the journal commit before returning. If the server is running without journaling, it returns immediately, and successfully.
 */
class WriteConcern(val w: Int = 1, val wTimeout: Int = 0, val fsync: Boolean = false, val j: Boolean = false) {
  lazy val safe_? = w > 0

  // todo - implement me
  lazy val ignoreNetworkErrors_? = w < 0

  lazy val asDocument = Document("w" -> w, "wtimeout" -> wTimeout, "fsync" -> fsync, "j" -> j)

  lazy val blockingWrite_? = safe_? || j || fsync
}


/**
  * Helper class for creating WriteConcern instances
  *
  * @since 2.0
  * @see com.mongodb.WriteConcern
  */
object WriteConcern {

  /**
    * Exceptions are raised for network issues and server errors; Write operations wait for the server to flush data to disk
    */
  case object FsyncSafe extends WriteConcern(w = 1, fsync = true)

  /**
    * Exceptions are raised for network issues and server errors; waits for at least 2 servers for the write operation.
    */
  case object ReplicasSafe extends WriteConcern(w = 2)

  /**
    * Exceptions are raised for network issues and server errors; waits on a server for the write operation
    */
  case object Safe extends WriteConcern(w = 1)

  /**
    * Exceptions are raised for network issues but not server errors.
    * (Default Behavior)
    */
  case object Normal extends WriteConcern

  case object Unsafe extends WriteConcern(w = 0)

  /**
    * Create a new WriteConcern object.
    *
    * <p> w represents # of servers:
    * <ul>
    * <li>{@code w=-1} None, no checking is done</li>
    * <li>{@code w=0} None, network socket errors raised</li>
    * <li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
    * <li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
    * </ul>
    * </p>
    * @param w (Int) Specifies the number of servers to wait for on the write operation, and exception raising behavior. Defaults to { @code 0}
    * @param wTimeout (Int) Specifies the number MS to wait for the server operations to write.  Defaults to 0 (no timeout)
    * @param fsync (Boolean) Indicates whether write operations should require a sync to disk. Defaults to False
    */
  def apply(_w: Int = 0, _wTimeout: Int = 0, _fsync: Boolean = false, _j: Boolean = false) =
    new WriteConcern(w = _w, wTimeout = _wTimeout, fsync = _fsync, j = _j)
}

