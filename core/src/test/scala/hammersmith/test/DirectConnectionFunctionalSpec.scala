
package hammersmith.test

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith
import org.specs2.Specification
import hammersmith.util.Logging
import akka.actor.{Props, ActorSystem}
import hammersmith.DirectMongoDBConnector
import java.net.{InetSocketAddress, InetAddress}
import akka.testkit.TestActorRef

@RunWith(classOf[JUnitRunner])
class DirectConnectionFunctionalSpec extends Specification with Logging {

  def is = sequential ^
    "This is a functional specification testing the direct connection in Hammersmith" ^
    p ^
    "The Direct Connection for Hammersmith should " ^
      "Connect to MongoDB" ! testMongoDBConnection ^
  endp

  def testMongoDBConnection = {
    implicit val system = ActorSystem("direct-connection-test")
    val conn = system.actorOf(Props(classOf[DirectMongoDBConnector], new InetSocketAddress("localhost", 27017), true))
    Thread.sleep(1000)
    conn must not beNull
  }

}
