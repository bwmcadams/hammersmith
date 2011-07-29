package com.mongodb.async
package util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ThreadFactory

object ThreadFactories {

  private class NamedThreadFactory(val name: String)
      extends ThreadFactory {

    private val nextSerial = new AtomicInteger(1)

    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setName("%s %d".format(name, nextSerial.incrementAndGet))
      t
    }
  }

  def apply(name: String): ThreadFactory = {
    new NamedThreadFactory(name)
  }
}
