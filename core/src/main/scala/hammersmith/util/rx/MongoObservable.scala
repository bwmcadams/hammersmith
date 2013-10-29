
package hammersmith.util.rx

import hammersmith.bson.SerializableBSONObject
import scala.collection.concurrent.TrieMap

/**
 * A reactive Observable inspired by Netflix's RxJava (and scala adapter)
 *
 * Designed to be the replacement for Iterators on a MongoDB Cursor,
 * instead providing the equivalent of a "Reactive Stream".
 *
 * Subscribe to the Observable, and your function or MongoObserver[T] will be invoked
 * each time an item becomes available.
 *
 * This proves more accessible than Iteratees, and hides complex implementation details such
 * as GetMore or even tailable cursors.
 *
 * @see https://github.com/Netflix/RxJava/tree/master/language-adaptors/rxjava-scala
 */
trait MongoObservable[+T] {
  def subscribe(onNext: T => Unit): MongoSubscription

  def subscribe(onNext: T => Unit, onError: Throwable => Unit): MongoSubscription

  def subscribe(onNext: T => Unit, onComplete: () => Unit, onError: Throwable => Unit): MongoSubscription

  def subscribe(observer: MongoObserver[T]): MongoSubscription
}

trait MongoObserver[-T] {
  /**
   * What to do in the case of an error.
   *
   * Once this is invoked, no further calls to `onNext` will be made,
   * and `onComplete` will not be invoked.
   *@param t
   */
  def onError(t: Throwable): Unit

  /**
   * Indicates that the data stream inside the Observable has ended,
   * and no more data will be send (i.e. no more calls to `onNext`, and `onError`
   * will not be invoked)
   *
   * This is especially useful with something like a Cursor to indicate
   * that the total data stream has been exhausted.
   */
  def onComplete(): Unit

  def onNext(item: T): Unit
}

sealed class AnonymousObserver[-T](nextFunc: T => Unit,
                                   errorFunc: Throwable => Unit = { t => () },
                                   completeFunc: () => Unit = { () => () }) extends MongoObserver[T] {
  def onError(t: Throwable) = errorFunc
  def onComplete = completeFunc
  def onNext(item: T) = nextFunc
}

trait MongoSubscription {
  def unsubscribe(): Unit
}
