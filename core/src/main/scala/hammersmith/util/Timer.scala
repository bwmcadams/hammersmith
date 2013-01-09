/**
 * Copyright (c) 2011-2013 Brendan W. McAdams <http://evilmonkeylabs.com>
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
package hammersmith
package util

/** 
 * Based on the timers in the Twitter-util library
 */
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CancellationException, ExecutorService, RejectedExecutionHandler,
  ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.collection.mutable.ArrayBuffer

trait TimerTask {
  def cancel()
}

trait Timer {
  
  def schedule(millisFromNow: Long)(f: => Unit): TimerTask 

  def stop()
}

class JavaTimer(isDaemon: Boolean) extends Timer {
  def this() = this(false)

  private[this] val underlying = new java.util.Timer(isDaemon)

  def schedule(millisFromNow: Long)(f: => Unit) = {
    val task = toJavaTimerTask(f)
    underlying.scheduleAtFixedRate(task, millisFromNow, millisFromNow)
    toTimerTask(task)
  }


  def stop() = underlying.cancel()

  /**
   * log any Throwables caught by the internal TimerTask.
   *
   * By default we log to System.err but users may subclass and log elsewhere.
   *
   * This method MUST NOT throw or else your Timer will die.
   */
  def logError(t: Throwable) {
    System.err.println("WARNING: JavaTimer caught exception running task: %s".format(t))
    t.printStackTrace(System.err)
  }

  private[this] def toJavaTimerTask(f: => Unit) = new java.util.TimerTask {
    def run {
      try {
        f
      } catch {
        case NonFatal(t) => logError(t)
        case fatal =>
          logError(fatal)
          throw fatal
      }
    }
  }

  private[this] def toTimerTask(task: java.util.TimerTask) = new TimerTask {
    def cancel() { task.cancel() }
  }
}


/**
 * A classifier of fatal exceptions -- identical in behavior to
 * the upcoming [[scala.util.control.NonFatal]] (which appears in
 * scala 2.10).
 */
object NonFatal {
  /**
   * Determines whether `t` is a fatal exception.
   *
   * @return true when `t` is '''not''' a fatal exception.
   */
  def apply(t: Throwable): Boolean = t match {
    // StackOverflowError ok even though it is a VirtualMachineError
    case _: StackOverflowError => true
    // VirtualMachineError includes OutOfMemoryError and other fatal errors
    case _: VirtualMachineError | _: ThreadDeath | _: InterruptedException |
      _: LinkageError  => false
    case _ => true
  }

  /**
   * A deconstructor to be used in pattern matches, allowing use in exception
   * handlers.
   *
   * {{{
   * try dangerousOperation() catch {
   *   case NonFatal(e) => log.error("Chillax")
   *   case e => log.error("Freak out")
   * }
   * }}}
   */
  def unapply(t: Throwable): Option[Throwable] = if (apply(t)) Some(t) else None
}
