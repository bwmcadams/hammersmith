
package hammersmith.util.rx

import hammersmith.bson.SerializableBSONObject
import scala.collection.concurrent.TrieMap
import akka.actor.ActorRef
import hammersmith.util.rx.operators._

/**
 * defines an onSubscribe func
 * @tparam T
 */
trait OnSubscribe[T] extends ((MongoObserver[T]) => MongoSubscription) {
  // this is our "onSubscribe" function
  def apply(observer: MongoObserver[T]): MongoSubscription
}

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
abstract class MongoObservable[+T](onSubscribe: OnSubscribe[T]) {

  // todo - store query

  // Factory function for creating clones of this
  def apply[T](onSubscribe: OnSubscribe[T]): MongoObservable[T]

  /**
   * Subscribe to events from the source.
   * Note that invoking this causes a Query to be executed against MongoDB.
   */
  def subscribe(onNext: T => Unit): MongoSubscription

  /**
   * Subscribe to events from the source.
   * Note that invoking this causes a Query to be executed against MongoDB.
   */
  def subscribe(onNext: T => Unit, onError: Throwable => Unit): MongoSubscription

  /**
   * Subscribe to events from the source.
   * Note that invoking this causes a Query to be executed against MongoDB.
   */
  def subscribe(onNext: T => Unit, onComplete: () => Unit, onError: Throwable => Unit): MongoSubscription

  /**
   * Subscribe to events from the source.
   * Note that invoking this causes a Query to be executed against MongoDB.
   */
  def subscribe(observer: MongoObserver[T]): MongoSubscription

  /**
   * Returns a cached Observable that can replay its items to a future observer
   * @return
   */
  // todo - implement
  //def cache: MongoObservable: T
  /**
   * Returns a cached Observable that can replay its items to a future observer,
   * after the start function is invoked
   * @return
   */
  // todo - implement
  //def replay: (() => MongoSubscription, MongoObservable): T

  /**
   * Concatenates two observables together and emits all the items emitted by them one after another
   */
  // todo - implement
  //def concat[U]: MongoObservable[U]

  /**
   * Returns an observable that forwards any distinct items that are emitted from the source,
   * according to a Key selection function
   *
   * NOTE: You should be using the distinct function on MongoCollection for optimal performance
   *
   * @see http://docs.mongodb.org/manual/reference/method/db.collection.distinct/
   */
  def distinct[U](f: (T) => U): MongoObservable[T] = apply(DistinctOperator(this, f))

  /**
   * Returns an observable that forwards any distinct items that are emitted from the source
   *
   * NOTE: You should be using the distinct function on MongoCollection for optimal performance
   *
   * @see http://docs.mongodb.org/manual/reference/method/db.collection.distinct/
   */
  def distinct: MongoObservable[T] = apply(DistinctOperator(this))


  // todo - distinctUntilChanged

  /**
   * Returns a MongoObservable that skips the first n items.
   *
   * Note that this causes a new query to be executed on the server,
   * to allow us to do the 'skip' in MongoDB. (this behavior may change in the future)
   *
   * TODO - implement the serverside call
   */
  def drop(n: Int): MongoObservable[T]

  /**
   * Returns an Observable that skips all items from the source as long as the specified
   * predicate is true
   *
   *  This is currently done client side: we may offer a Mongo Query function later for it.
   */
  def dropWhile(p: (T) => Boolean): MongoObservable[T] = apply(DropWhileOperator[T](this, p))

  /**
   * Tests whether a predicate holds true for elements of the MongoObservable,
   * returning a new MongoObservable of Boolean
   *
   * NOTE: the elements won't be returned, just booleans, and it is done clientSide. You probably want
   * to use the MongoDB $exists operator in your query instead...
   *
   * @see http://docs.mongodb.org/manual/reference/operator/query/exists/
   */
  def exists(p: (T) => Boolean): MongoObservable[Boolean] = apply(ExistsOperator(this, p))

  /**
   * Returns a MongoObservable which will only emit items that pass a predicate function
   *
   * NOTE: This op is run clientSide, post-query; you probably want to do this as a MongoDB query for maximum performance
   */
  def filter(p: (T) => Boolean): MongoObservable[T] = apply(FilterOperator(this, p))

  /**
   * Registers a function which will be called when the MongoObservable invokes onComplete or onError
   * @param action
   * @return
   */
  def finallyDo(action: () => Unit): MongoObservable[T] = apply(FinallyOperator(this, action))

  /**
   * Returns a MongoObservable with just the first item in the Observable.
   *
   * Also exposed by Collection.first(q)
   *
   * @see head
   * @return
   */
  def first: MongoObservable[T] = head

  /**
   * Returns an MongoObservable with just the first item, or a defualt
   * value if the source is empty.
   *
   * @see headOrElse
   * @param default
   * @tparam U
   * @return
   */
  def firstOrElse[U >: T](default: => U): MongoObservable[U] = headOrElse(default)

  /**
   * Creates a new [MongoObservable] by applying a function supplied to each item
   * emitted by the source, where that function returns a [MongoObservable].
   * Then merges the resulting [MongoObservable]s & emits the results of the merger.
   */
  def flatMap[R](f: (T) => MongoObservable[R]): MongoObservable[R]

  /**
   * Returns a [MongoObservable] which applies a function to the first item emitted by the source
   * then feeds the results, along with the second item into the function, and so on until
   * all items have been emitted, sending the final result to your function as its sole item.
   */
  // in rxJava Just calls reduce.
  def foldLeft[R](initial: R)(f: (R, T) => R): MongoObservable[R]


  /**
   * Returns a [MongoObservable] which emits a single Boolean indicating whether all of the items
   * emitted by the source satisfy a condition.
   *
   * NOTE: Like exists this returns a [MongoObservable] of boolean, not T, so you won't get the items out
   */
  def forall(f: (T) => Boolean): MongoObservable[Boolean]

  /**
   * Groups the items emitted by the [MongoObservable] according to your specified
   * discrimination function
   */
  def groupBy[K](f: (T) => K): MongoObservable[(K, MongoObservable[T])]


  /**
   * Returns a MongoObservable with just the first item in the Observable.
   *
   * Also exposed by Collection.first(q)
   *
   * @return
   */
  def head: MongoObservable[T]

  /**
   * Returns a [MongoObservable] that emits only the first item emitted by the source,
   * or a default value if the source is empty
   */
  def headOrElse[U >: T](default: => U): MongoObservable[U]

  /**
   * Tests whether the source QUERY emits no results.
   *
   * NOTE: This requires a Mongo query to be run using count()
   * @return
   */
  def isEmpty: MongoObservable[Boolean]

  /**
   * Tests the length of the source QUERY
   *
   * NOTE: This requires a Mongo query to be run using count()
   * @return
   */
  def length: MongoObservable[Int]


  /**
   * Returns a [MongoObservable] which applies the given function to
   * each item emitted by the source, emitting the result
   */
  def map[R](f: (T) => R): MongoObservable[R]


  /**
   * Returns a [MongoObservable] which applies a function to the first
   * item emitted by the soruce, then feeds the result of that function
   * along with the second item into the same function, etc etc until all
   * items have been emitted by the source, emitting the final result to your
   * function as its sole item
   */
  def reduce[U >: T](f: (U, U) => U): MongoObservable[U]

  /**
   * Returns a [MongoObservable] which applies a function to the first item emitted
   * by the source, then feeds its results along w/ second item etc etc into function
   * emitting the result of each of these iterations
   */
  def scan[R](init: R)(f: (R, T) => R): MongoObservable[R]

  /**
   * Tests the length of the source QUERY
   *
   * NOTE: This requires a Mongo query to be run using count()
   * @return
   */
  def size: MongoObservable[Int] = length

  /**
   * Returns a [MongoObservable] that emits only the first n items emitted by the source
   */
  def take(n: Int): MongoObservable[T]

  /**
   * Returns a [MongoObservable] that emits only the last n items emitted by the source
   */
  def takeRight(n: Int): MongoObservable[T]

  /**
   * Returns a [MongoObservable] that emits items emitted by the source so long
   * as a specified condition is true
   */
  def takeWhile(f: (T) => Boolean): MongoObservable[T]

  /**
   * Returns a [MongoObservable] which emits a single item, a list composed by all items
   * emitted by the source
   */
  def toSeq: MongoObservable[Seq[T]]


  // todo - zip / zipWithIndex

  /*
  def withFilter(p: (T) => Boolean): WithFilter[T]


  private class WithFilter[+T] (p: T => Boolean, o: MongoObservable[_ <: T]) {

    def map[B](f: T => B): MongoObservable[B] = {
      MongoObservable[B](o.filter(p).map[B](f))
    }

    def flatMap[B](f: T => MongoObservable[B]): MongoObservable[B] = {
      MongoObservable[B](o.filter(p).flatMap[B]((x: T) => f(x)))
    }

    def withFilter(q: T => Boolean): MongoObservable[T] = {
      MongoObservable[T](o.filter((x: T) => p(x) && q(x)))
    }

  } */
}

object MongoObservable {

  trait SubscriptionMessage
  case class SubscriptionStarted[T](observable: MongoObservable[T], onSubscribe: (T) => Unit)


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
  def onError(t: Throwable) = errorFunc(t)
  def onComplete = completeFunc()
  def onNext(item: T) = nextFunc(item)
}

trait MongoSubscription {
  def unsubscribe(): Unit
}
