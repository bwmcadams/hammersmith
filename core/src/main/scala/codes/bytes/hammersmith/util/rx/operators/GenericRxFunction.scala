
package codes.bytes.hammersmith.util.rx
package operators

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success, Try}


trait RxOperator[T] extends OnSubscribe[T]

protected[rx] class GenericRxOperator[T](source: MongoObservable[T], p: (T) => Boolean, onEmpty: Boolean = false) extends RxOperator[Boolean] {

  override def apply(observer: MongoObserver[Boolean]) = {
    val subscription = SafeMongoSubscription()

    subscription.wrap(source.subscribe(new MongoObserver[T] {
      val emitted: AtomicBoolean = new AtomicBoolean(false)


      def onComplete(): Unit = if (!emitted.get) {
        observer.onNext(onEmpty) // what's our "default" if empty
        observer.onComplete()
      }


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

