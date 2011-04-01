import sbt._
import growl._
import com.github.olim7t.sbtscalariform._

class MongoScalaNativeProject(info: ProjectInfo)
  extends ParentProject(info) 
  with IdeaProject
  with posterous.Publish {

  override def managedStyle = ManagedStyle.Maven

  lazy val bson = project("bson-driver", "bson-driver", new BSONDriverProject(_))
  lazy val mongo = project("mongo-driver", "mongo-driver", new MongoDriverProject(_))

  abstract class NativeBaseProject(info: ProjectInfo) 
    extends DefaultProject(info)
    with AutoCompilerPlugins
    with IdeaProject
    with ScalariformPlugin
    with GrowlingTests {

    override def scalariformOptions = Seq(VerboseScalariform)

    // Testing Deps
    val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test->default"
    val slf4j = "org.slf4j" % "slf4j-api" % "1.6.0"
    // JCL bindings for testing only
    val slf4jJCL = "org.slf4j" % "slf4j-jcl" % "1.6.0" % "test"
    
  }


  class BSONDriverProject(info: ProjectInfo) extends NativeBaseProject(info) {
    // For testing BSON wire formats etc from a 'known good' state
    val mongoJava = "org.mongodb" % "mongo-java-driver" % "2.5.2" % "test->default"
  }

  class MongoDriverProject(info: ProjectInfo) extends NativeBaseProject(info)
  


}

// vim: set ts=2 sw=2 sts=2 et:
