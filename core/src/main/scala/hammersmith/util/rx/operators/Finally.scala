
package hammersmith.util.rx
package operators

object FinallyOperator {
  def apply[T](source: MongoObservable[T], action: () => Unit) =
    new FinallyOperator[T](source, action)
}
class FinallyOperator[T] private(source: MongoObservable[T], action: () => Unit) extends RxOperator[T] {
  // this is our "onSubscribe" function
  def apply(observer: MongoObserver[T]): MongoSubscription = source.subscribe( new MongoObserver[T] {
    def onComplete(): Unit = try {
      observer.onComplete()
    } finally {
      action()
    }

    def onError(t: Throwable): Unit = try {
      observer.onError(t)
    } finally {
      action()
    }

    def onNext(item: T): Unit = try {
      observer.onNext(item)
    }
  })
}
