
package hammersmith.util.rx
package operators

class ExistsOperator[T](source: MongoObservable[T], p: (T) => Boolean) extends GenericRxOperator[T](source, p, false) {
