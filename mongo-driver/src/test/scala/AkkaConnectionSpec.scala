import akka.actor.ActorRef
import com.mongodb.async._
import com.mongodb.async.util._
import com.mongodb.async.futures.RequestFutures
import org.bson.collection._
import org.specs2.time.Time._
import org.bson.util.Logging
import org.bson.types._
import org.specs2.execute.Result
import org.specs2.Specification
import org.specs2.specification._
import org.specs2.matcher._

trait HammersmithDefaultDBNames {
  val integrationTestDBName = "hammersmithIntegration"

}

class AkkaConnectionSpec extends Specification
with Logging
with HammersmithDefaultDBNames {

  def is =
    "The MongoDB Direct Connection" ^
      "Connect correctly and grab isMaster, then disconnect" ! connectIsMaster ^
      end


  def connectIsMaster = {
    val x = AkkaConnection()
    Thread.sleep(10000)
    x must not beNull
  }

}
