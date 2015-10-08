
package codes.bytes.hammersmith.io

import codes.bytes.hammersmith.util.rx._
import scala.collection.concurrent.TrieMap
import codes.bytes.hammersmith.wire.{QueryMessage, ReplyMessage}
import akka.actor.{ActorLogging, Actor, ActorRef}
import codes.bytes.hammersmith.{GetMoreRequest, MongoException, MongoMutationRequest}
import codes.bytes.hammersmith.collection.Implicits._
import codes.bytes.hammersmith.collection.immutable.{Document => ImmutableDocument}
import codes.bytes.hammersmith.bson.SerializableBSONObject


/**
 * This actor proxies the behavior of a cursor so that the actual fetching and pushing can be done
 * on a separate thread...
 * Mongo pins connections to specific connections, so we need to reuse that connection
 *
 * TODO - Cursor Cleaner
 */
/*
class MongoCursorActor[T : SerializableBSONObject](conn: ActorRef, cursor: MongoCursor[T]) extends Actor with ActorLogging {
  import MongoCursor._


  // This should keep it nice and serial
  def receive: Actor.Receive = {
    case PushEntries(r, w) =>
      pushCursorEntries(r, w)
    case NextBatch(r, w) =>
      pushCursorEntries(r, w)
  }

  def pushCursorEntries(r: ReplyMessage, w: MongoMutationRequest) {
    /**
     * Cursor ID 0 indicates "No more results"
     * HOWEVER - Cursors can be positive OR negative
     * If no more, can't getMore, and onComplete() at end.
     */
    val hasMore = r.cursorID == 0
    /**
     * Batch size: Defaults to 0 which allows Mongo to control the size
     * TODO: Make this controllable from outside?
     */
    val batchSize = w.msg match {
      // don't rely on numReturned from reply as it may be skewed
      case q: QueryMessage => q.numberToReturn
      case _ => 0
    }

    if (r.cursorNotFound)
      cursor.publishError(new CursorNotFoundException)
    else if (r.queryFailure) {
      val err: String = r.documents[ImmutableDocument].head.getAs[String]("$err") match {
        case Some(errMsg) => errMsg
        case None => " { No specific error detail reported by server } "
      }
      cursor.publishError(new QueryFailedException(err))
    }
    else if (r.awaitCapable == false)
      cursor.publishError(new AwaitUnsupportedException)
    else {
      val documents = r.documents[T]

      documents foreach { cursor.publishNext }

      if (hasMore) {
        // get more from ye ol' mongo server
        conn ! GetMoreRequest[T](w.msg.namespace, batchSize, r.cursorID)
      }
      else {
        // shut ourselves down, since the cursor is completed
        cursor.publishCompleted()
        context.stop(self)
      }

    }
  }
}


object MongoCursor {
  sealed trait MongoCursorMessage
  case class PushEntries(r: ReplyMessage, w: MongoMutationRequest)
  case class NextBatch(r: ReplyMessage, w: MongoMutationRequest)

  class CursorNotFoundException extends MongoException("Requested Cursor Not Found On Server.")
  class QueryFailedException(err: String) extends MongoException("Server Reported Query Failure: '%s'".format(err))
  class AwaitUnsupportedException extends MongoException("Server reports AwaitData unsupported. Possibly old version of Mongo, please ensure server v1.8+.")
}

// TODO - This needs to be able to communicate with its parent actor
class MongoCursor[T](r: ReplyMessage, w: MongoMutationRequest) extends MongoObservable[T] {
  self =>

  val subscriptions = TrieMap.empty[MongoSubscription, MongoObserver[T]]

  def subscribe(observer: MongoObserver[T]): MongoSubscription = {
    val subscription = new MongoSubscription {
      def unsubscribe(): Unit = subscriptions -= this
    }
    subscriptions += subscription -> observer

    subscription

  }

  def subscribe(onNext: (T) => Unit): MongoSubscription = subscribe(new AnonymousObserver[T](onNext))

  def subscribe(onNext: (T) => Unit, onComplete: () => Unit, onError: (Throwable) => Unit): MongoSubscription =
    subscribe(new AnonymousObserver[T](onNext, onError, onComplete))

  def subscribe(onNext: (T) => Unit, onError: (Throwable) => Unit): MongoSubscription =
    subscribe(new AnonymousObserver[T](onNext, onError))


  private[io] def publishNext(item: T) = {
    subscriptions foreach { s_o =>
      s_o._2.onNext(item)
    }
  }

  private[io] def publishError(error: Throwable) = {
    subscriptions foreach { s_o =>
      s_o._2.onError(error)
    }

  }

  private[io] def publishCompleted() = {
    subscriptions foreach { s_o =>
      s_o._2.onComplete()
    }
  }

}
*/
