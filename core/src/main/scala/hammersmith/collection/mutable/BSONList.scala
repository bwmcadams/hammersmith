
package hammersmith.bson.collection.mutable


import scala.collection.mutable._
import scala.collection.JavaConverters._
import collection.parallel.mutable

class BSONList(val underlying: ListBuffer[Any] = ListBuffer.empty[Any]) extends Seq[Any] {

  def apply(i: Int) = underlying(i)

  def update(i: Int, elem: Any) =
    underlying.update(i, elem)

  def +=(elem: Any): this.type = {
    underlying += elem
    this
  }

  def insertAll(i: Int, elems: scala.Traversable[Any]) = {
    val ins = underlying.subList(0, i)
    elems.foreach(x => ins.add(x.asInstanceOf[AnyRef]))
  }

  def remove(i: Int) = underlying.remove(i)

  /**
   * as
   *
   * Works like apply(), unsafe, bare return of a value.
   * Returns default if nothing matching is found, else
   * tries to cast a value to the specified type.
   *
   * Unless you overrode it, default throws
   * a NoSuchElementException
   *
   * @param idx (Int)
   * @tparam A
   * @return (A)
   * @throws NoSuchElementException
   */

  def as[A : NotNothing](idx: Int): A = {
    underlying.get(idx) match {
      case null => throw new NoSuchElementException
      case value => value.asInstanceOf[A]
    }
  }

  /** Lazy utility method to allow typing without conflicting with Map's required get() method and causing ambiguity */
  def getAs[A : NotNothing](idx: Int): Option[A] = {
    underlying.get(idx) match {
      case null => None
      case value => Some(value.asInstanceOf[A])
    }
  }

  def getAsOrElse[A : NotNothing](idx: Int, default: => A): A = getAs[A](idx) match {
    case Some(v) => v
    case None => default
  }

  def clear = underlying.clear

  def result = this

  def length = underlying.size

  override def isEmpty = underlying.isEmpty
  override def iterator = underlying.iterator.asScala

  override def toString() = underlying.toString
  override def hashCode() = underlying.hashCode
  override def equals(that: Any) = that match {
    case o: MongoDBObject => underlying.equals(o.underlying)
    case o: MongoDBList => underlying.equals(o.underlying)
    case _ => underlying.equals(that) | that.equals(this)
  }
}

object MongoDBList {

  def empty: MongoDBList = new MongoDBList()

  def apply[A <: Any](elems: A*): MongoDBList = {
    val b = newBuilder[A]
    for (xs <- elems) xs match {
      case p: Tuple2[String, _] => b += MongoDBObject(p)
      case _ => b += xs
    }
    b.result
  }

  def concat[A](xss: scala.Traversable[A]*): MongoDBList = {
    val b = newBuilder[A]
    if (xss forall (_.isInstanceOf[IndexedSeq[_]]))
      b.sizeHint(xss map (_.size) sum)

    for (xs <- xss) b ++= xs
    b.result
  }

  def newBuilder[A <: Any]: MongoDBListBuilder = new MongoDBListBuilder

}

sealed class MongoDBListBuilder extends scala.collection.mutable.Builder[Any, Seq[Any]] {

  protected val empty: MongoDBList = new MongoDBList

  protected var elems: MongoDBList  = empty

  override def +=(x: Any) = {
    val v = x match {
      case _ => x.asInstanceOf[AnyRef]
    }
    elems.add(v)
    this
  }

  def clear() { elems = empty }

  def result: MongoDBList = elems
}

