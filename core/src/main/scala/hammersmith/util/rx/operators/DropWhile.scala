
package hammersmith.util.rx
package operators

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Try, Success, Failure}


class DropWhileOperator[T](source: MongoObservable[T], p: (T) => Boolean) extends RxOperator[T] {

  override def apply(source: MongoObservable[T], p: (T) => Boolean) = {
    source.subscribe(new DropWhileObserver(observer))
  }

  private class DropWhileObserver(observer: MongoObserver[T]) extends MongoObserver[T] {

    val skipping_? = new AtomicBoolean(true)

    /**
     * Indicates that the data stream inside the Observable has ended,
     * and no more data will be send (i.e. no more calls to `onNext`, and `onError`
     * will not be invoked)
     *
     * This is especially useful with something like a Cursor to indicate
     * that the total data stream has been exhausted.
     */
    def onComplete(): Unit = observer.onComplete()

    /**
     * What to do in the case of an error.
     *
     * Once this is invoked, no further calls to `onNext` will be made,
     * and `onComplete` will not be invoked.
     * @param t
     */
    def onError(t: Throwable): Unit = observer.onError(t)

    def onNext(item: T): Unit = {
      if (!skipping.get) observer.onNext(item)
      else {
        Try(
          if (!f(item))  {
            skipping.set(false)
            observer.onNext(item)
          } else {}
        ) match {
          case Success(_) =>
          case Failure(t) => observer.onError(t)
        }
      }
    }
  }

}
