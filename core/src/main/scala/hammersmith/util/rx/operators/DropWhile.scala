
package hammersmith.util.rx
package operators

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Try, Success, Failure}


object DropWhileOperator {
  def apply[T](source: MongoObservable[T], p: (T) => Boolean) = {
    new DropWhileOperator[T](source, p)
  }
}

class DropWhileOperator[T] private(source: MongoObservable[T], p: (T) => Boolean) extends RxOperator[T] {

  override def apply(observer: MongoObserver[T]) = {
    source.subscribe(new MongoObserver[T] {

      val skipping = new AtomicBoolean(true)

      def onComplete(): Unit = observer.onComplete()

      def onError(t: Throwable): Unit = observer.onError(t)

      def onNext(item: T): Unit = {
        if (!skipping.get) observer.onNext(item)
        else {
          Try(
            if (!p(item))  {
              skipping.set(false)
              observer.onNext(item)
            } else {}
          ) match {
            case Success(_) =>
            case Failure(t) => observer.onError(t)
          }
        }
      }
    })
  }
}
