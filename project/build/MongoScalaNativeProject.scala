import sbt._
import growl._
import com.github.olim7t.sbtscalariform._

class MongoScalaNativeProject(info: ProjectInfo)
  extends ParentProject(info) 
  with IdeaProject
  with posterous.Publish {

  override def managedStyle = ManagedStyle.Maven

  lazy val bson = project("bson-driver", "bson-driver", new BSONDriverProject(_))
  lazy val mongo = project("mongo-driver", "mongo-driver", new MongoDriverProject(_), bson)

  abstract class NativeBaseProject(info: ProjectInfo) 
    extends DefaultProject(info)
    with AutoCompilerPlugins
    with IdeaProject
    with ScalariformPlugin
    with GrowlingTests {

    override def scalariformOptions = Seq(VerboseScalariform)


    // Use the BSON code
//    val jBSON = "org.mongodb" % "bson" % "2.5.2"
    // Connection Pooling
    val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"

    val scalaj_collection = "org.scalaj" % "scalaj-collection_2.8.0" % "1.0"
    // Netty
    val netty = "org.jboss.netty" % "netty" % "3.2.4.Final"
    // Testing Deps
    val specs2 = "org.specs2" %% "specs2" % "1.0.1"

    def specs2Framework = new TestFramework("org.specs2.runner.SpecsFramework")
    override def testFrameworks = super.testFrameworks ++ Seq(specs2Framework)

    val slf4j = "org.slf4j" % "slf4j-api" % "1.6.0"
    // JCL bindings for testing only
    val slf4jJCL = "org.slf4j" % "slf4j-jcl" % "1.6.0" % "test"
    
  }


  class BSONDriverProject(info: ProjectInfo) extends NativeBaseProject(info) {
    // For testing BSON wire formats etc from a 'known good' state
    // val mongoJava = "org.mongodb" % "mongo-java-driver" % "2.5.2" % "test->default"
  }

  class MongoDriverProject(info: ProjectInfo) extends NativeBaseProject(info)

  val sbtSnapshots = "snapshots" at "http://scala-tools.org/repo-snapshots"
  val sbtReleases  = "releases" at "http://scala-tools.org/repo-releases"

  val jbossRepo = "JBoss Public Repo" at "https://repository.jboss.org/nexus/content/groups/public-jboss/"
}

// vim: set ts=2 sw=2 sts=2 et:
