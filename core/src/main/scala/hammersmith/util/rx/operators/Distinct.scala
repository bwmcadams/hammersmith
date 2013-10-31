
package hammersmith.util.rx
package operators

import scala.collection.mutable


object DistinctOperator {
  def apply[T](source: MongoObservable[T]) = {
    new DistinctOperator[T, T](source, identity[T])
  }

  def apply[T, U](source: MongoObservable[T], f: (T) => U) = {
    new DistinctOperator[T, U](source, f)
  }
}
class DistinctOperator[T, U] private(source: MongoObservable[T], f: (T) => U) extends RxOperator[T] {

  override def apply(observer: MongoObserver[T]): MongoSubscription = {

    val sourceSubscription: MongoSubscription = source.subscribe(new MongoObserver[T] {
      // emitted Keys
      private val keys = mutable.HashSet.empty[U]

      /**
       * Indicates that the data stream inside the Observable has ended,
       * and no more data will be send (i.e. no more calls to `onNext`, and `onError`
       * will not be invoked)
       *
       * This is especially useful with something like a Cursor to indicate
       * that the total data stream has been exhausted.
       */
      def onComplete(): Unit = {
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
        val k = f(item)
        if (!keys.contains(k)) {
          keys += k
          observer.onNext(item)
        }
      }
    })

    new MongoSubscription {
      def unsubscribe() = sourceSubscription.unsubscribe()
    }
  }

}
