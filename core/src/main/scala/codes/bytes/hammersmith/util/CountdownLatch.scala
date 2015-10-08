/**
 * Copyright (c) 2011-2015 Brendan McAdams <http://bytes.codes>
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
package codes.bytes.hammersmith
package util


import java.util.concurrent.TimeUnit

/**
 * Based on the Twitter-Util CountdownLatch
 */
class CountdownLatch(val initialCount: Int) {
  val underlying = new java.util.concurrent.CountDownLatch(initialCount)
  def count = underlying.getCount
  def isZero = count == 0
  def countDown() = underlying.countDown()
  def await() = underlying.await()
  def await(timeout: Long) = underlying.await(timeout, TimeUnit.MILLISECONDS)
  def within(timeout: Long) = await(timeout) || {
    throw new Exception("Within Failed: " + timeout.toString)
  }
}
