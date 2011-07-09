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

/*
 * Note that no matter what any other settings are this is only called if
 * w > 0
 */
trait WriteConcern {
  /**
   * @param w (Int) Specifies the number of servers to wait for on the write operation, and exception raising behavior. Defaults to {@code 0}
   */
  val w: Int = 0
  /**
   * @param wTimeout (Int) Specifies the number MS to wait for the server operations to write.  Defaults to 0 (no timeout)
   */
  val wTimeout: Int = 0
  /**
   * @param fsync (Boolean) Indicates whether write operations should require a sync to disk. Defaults to False
   */
  val fsync: Boolean = false
  /**
   * @param j (Boolean) *1.9+ ONLY* When true, the getlasterror call awaits the journal commit before returning. If the server is running without journaling, it returns immediately, and successfully.
   */
  val j: Boolean = false

  lazy val safe_? = w > 0

  lazy val ignoreNetworkErrors_? = w < 0
}

/**
 * Helper class for creating WriteConcern instances
 *
 * @author Brendan W. McAdams <brendan@10gen.com>
 * @since 2.0
 * @see com.mongodb.WriteConcern
 */
object WriteConcern {
  /**
   * Exceptions are raised for network issues and server errors; Write operations wait for the server to flush data to disk
   */
  case object FsyncSafe extends WriteConcern {
    override val w = 1
    override val fsync = true
  }
  /**
   * Exceptions are raised for network issues and server errors; waits for at least 2 servers for the write operation.
   */
  case object ReplicasSafe extends WriteConcern {
    override val w = 2
  }

  /**
   * Exceptions are raised for network issues and server errors; waits on a server for the write operation
   */
  case object Safe extends WriteConcern {
    override val w = 1
  }

  /**
   * Exceptions are raised for network issues but not server errors.
   * (Default Behavior)
   */
  case object Normal extends WriteConcern

  /**
   * Create a new WriteConcern object.
   *
   * <p> w represents # of servers:
   * 		<ul>
   * 			<li>{@code w=-1} None, no checking is done</li>
   * 			<li>{@code w=0} None, network socket errors raised</li>
   * 			<li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
   * 			<li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
   * 		</ul>
   * 	</p>
   * @param w (Int) Specifies the number of servers to wait for on the write operation, and exception raising behavior. Defaults to {@code 0}
   * @param wTimeout (Int) Specifies the number MS to wait for the server operations to write.  Defaults to 0 (no timeout)
   * @param fsync (Boolean) Indicates whether write operations should require a sync to disk. Defaults to False
   */
  def apply(_w: Int = 0, _wTimeout: Int = 0, _fsync: Boolean = false, _j: Boolean = false) =
    new WriteConcern {
      override val w = _w
      override val wTimeout = _wTimeout
      override val fsync = _fsync
      override val j = _j
    }

}

