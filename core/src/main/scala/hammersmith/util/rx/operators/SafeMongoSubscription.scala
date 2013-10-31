package hammersmith.util.rx
package operators

import java.util.concurrent.atomic.AtomicReference


/**
 * Threadsafe wrapper around subscription to ensure unsubscribe is called only once.
 */
class SafeMongoSubscription extends MongoSubscription {

  import SafeMongoSubscription._

  def this(subscription: MongoSubscription) = {
    this
    actual.set(Some(subscription))
  }

  private val actual = new AtomicReference[Option[MongoSubscription]](None)

  /**
   * When an actual subscription exists, wrap it ( if not specified to constructor )
   *
   * @param subscription the subscription to wrap
   * @throws IllegalStateException if you try to set it more than once, or call this method when set by constructor
   *
   */
  def wrap(subscription: MongoSubscription) = {
    if (!actual.compareAndSet(None, Some(subscription))) {
      actual get match {
        case Some(Unsubscribed) =>
          subscription.unsubscribe
          this
        case _ =>
          throw new IllegalStateException("Cannot set subscription more than once.")
      }
    }
    this
  }

  def unsubscribe(): Unit = {
    // get the real subscription & set to None atomically so only called once
    actual.getAndSet(Some(Unsubscribed)) match {
      case None =>
      case Some(actual) => actual.unsubscribe()
    }
  }

  def isUnsubscribed: Boolean = actual.get() == Some(Unsubscribed)
}

object SafeMongoSubscription {
  val Unsubscribed = new MongoSubscription {
    def unsubscribe(): Unit = {}
  }
  def apply() = new SafeMongoSubscription()
  def apply(subscription: MongoSubscription) = new SafeMongoSubscription(subscription)
}