
package codes.bytes.hammersmith.util.rx
package operators

object ExistsOperator {
  def apply[T](source: MongoObservable[T], p: (T) => Boolean) = new GenericRxOperator[T](source, p, false)
}
