
package codes.bytes.hammersmith

import java.net.InetSocketAddress

import akka.actor.{ActorSystem, Props}
import codes.bytes.hammersmith.collection.Implicits._
import codes.bytes.hammersmith.collection.immutable.Document
import com.typesafe.scalalogging.{StrictLogging, LazyLogging}
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DirectConnectionFunctionalSpec extends Specification with StrictLogging {

  implicit val system = ActorSystem("direct-connection-test")
  val conn = system.actorOf(Props(classOf[DirectMongoDBConnector], new InetSocketAddress("localhost", 27017), true))

  def is = sequential ^
    "This is a functional specification testing the direct connection in Hammersmith" ^
    p ^
    "The Direct Connection for Hammersmith should " ^
      "Connect to MongoDB" ! testMongoDBConnection ^
      "Get a list of databases" ! testGetDatabases ^
  endp

  def testMongoDBConnection = {
    conn must not beNull
  }

  def testGetDatabases = {
    conn ! CommandRequest[Document]("admin", Document("listDatabases" -> 1))
    conn must not beNull
  }

}
