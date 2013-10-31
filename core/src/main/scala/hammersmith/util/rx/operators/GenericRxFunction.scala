
package hammersmith.util.rx
package operators

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}


trait RxOperator[T] extends OnSubscribe[T]

protected[rx] class GenericRxOperator[T](source: MongoObservable[T], p: (T) => Boolean, onEmpty: Boolean = false) extends RxOperator[Boolean] {

  override def apply(observer: MongoObserver[Boolean]) = {
    val subscription = SafeMongoSubscription()

    subscription.wrap(source.subscribe(new MongoObserver[T] {
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
    }))
  }
}

