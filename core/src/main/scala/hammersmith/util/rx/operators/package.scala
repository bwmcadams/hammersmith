package hammersmith.util.rx

import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

import scala.util.{Try, Success, Failure}
import akka.actor.FSM.UnsubscribeTransitionCallBack

package object operators {

  trait RxOperator[T] extends OnSubscribe[T]


  protected[rx] class GenericRxOperator[T](observer: MongoObserver[T], p: (T) => Boolean, onEmpty: Boolean = false) extends RxOperator[T] {

    override def apply(source: MongoObservable[T]) = {
      val subscription = SafeMongoSubscription()

      sub.wrap(new MongoObserver[T] {
        val emitted: AtomicBoolean = new AtomicBoolean(false)

        /**
         * Indicates that the data stream inside the Observable has ended,
         * and no more data will be send (i.e. no more calls to `onNext`, and `onError`
         * will not be invoked)
         *
         * This is especially useful with something like a Cursor to indicate
         * that the total data stream has been exhausted.
         */
        def onComplete(): Unit = if (!emitted.get) {
          observer.onNext(onEmpty) // what's our "default" if empty
          observer.onComplete()
        }

        /**
         * What to do in the case of an error.
         *
         * Once this is invoked, no further calls to `onNext` will be made,
         * and `onComplete` will not be invoked.
         * @param t
         */
        def onError(t: Throwable): Unit = {
          observer.onError(t)
        }

        def onNext(item: T): Unit = {
          Try(
            if (!emitted.get())
              if (p(item) && !emitted.getAndSet(true)) {
                observer.onNext(!onEmpty)
                observer.onComplete()
                subscription.unsubscribe()
              }
          ) match {
            case Success(_) =>
            case Failure(t) =>
              observer.onError(t)
              subscription.unsubscribe()
          }
        }
      })
    }
  }

  /**
   * Threadsafe wrapper around subscription to ensure unsubscribe is called only once.
   */
  class SafeMongoSubscription extends MongoSubscription {

    import SafeMongoSubscription._

    def this(subscription: MongoSubscription) = {
      this
      actual.set(Some(subscription))
    }

    private val actual = new AtomicReference[Option[SafeMongoSubscription]] = None

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
      actual.getAndSet(Unsubscribed) match {
        case null =>
        case actual => actual.unsubscribe()
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
}
