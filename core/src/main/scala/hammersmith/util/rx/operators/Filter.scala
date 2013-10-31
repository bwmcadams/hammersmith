
package hammersmith.util.rx
package operators


import scala.util.{Try, Success, Failure}

object FilterOperator {
  def apply[T](source: MongoObservable[T], p: (T) => Boolean) =
    new FilterOperator[T](source, p)
}

class FilterOperator[T] private(source: MongoObservable[T], p: (T) => Boolean) extends RxOperator[T] {

  override def apply(observer: MongoObserver[T]): MongoSubscription = {

    val subscription = SafeMongoSubscription()

    subscription.wrap(source.subscribe(new MongoObserver[T] {

      def onComplete(): Unit = observer.onComplete()


      def onError(t: Throwable): Unit = observer.onError(t)

      def onNext(item: T): Unit = Try(
        if (p(item))
          observer.onNext(item)
      ) match {
        case Success(_) =>
        case Failure(t) =>
          observer.onError(t)
      }
    }))
  }

}
