import sbt._
import Keys._
/*import scalariform.formatter.preferences._*/

object HammersmithBuild extends Build {
  import Dependencies._
  import Resolvers._
  import Publish._

  lazy val buildSettings = Seq(
    organization := "net.evilmonkeylabs",
    version := "0.3.0-SNAPSHOT",
    scalaVersion := "2.10.0"/*,
    crossScalaVersions := Seq("2.9.2", "2.9.1")*/
  )

  /**
   * Import some sample data for testing
   */
  "mongoimport -d hammersmithIntegration -c yield_historical.in --drop ./core/src/test/resources/yield_historical_in.json" !

  "mongoimport -d hammersmithIntegration -c books --drop ./core/src/test/resources/bookstore.json" ! 

  override lazy val settings = super.settings ++ buildSettings

  lazy val baseSettings = Defaults.defaultSettings  ++ Publish.settings

  lazy val parentSettings = baseSettings

/*
 *  lazy val formatSettings = ScalariformPlugin.settings ++ Seq(
 *    formatPreferences in Compile := formattingPreferences,
 *    formatPreferences in Test    := formattingPreferences
 *  )
 *
 *  def formattingPreferences = {
 *    import scalariform.formatter.preferences._
 *    FormattingPreferences().setPreference(AlignParameters, true).
 *                            setPreference(DoubleIndentClassDeclaration, true).
 *                            setPreference(IndentLocalDefs, true).
 *                            setPreference(PreserveDanglingCloseParenthesis, true).
 *                            setPreference(RewriteArrowSymbols, true)
 *  }
 *
 */

  lazy val defaultSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq(commonsPool, netty, slf4j, akkaActors, specs2, junit),
    resolvers ++= Seq(sonaReleases, jbossRepo, twttrRepo, typesafeRepo),
    autoCompilerPlugins := true,
    parallelExecution in Test := true,
    testFrameworks += TestFrameworks.Specs2
  ) 

  lazy val hammersmith = Project(
    id = "hammersmith",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(core)
  )

  lazy val core = Project(
    id = "hammersmith-core",
    base = file("core"),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= Seq(bsonJava, mongoJava, slf4jJCL)
    )
  )

}

object Publish {
  lazy val settings = Seq(
    publishTo <<= version(v => Some(publishTarget(v))),
    credentials += Credentials(Path.userHome / ".ivy2" / ".scalatools_credentials")
  )

  private def publishTarget(version: String) = "Scala Tools Nexus" at "http://nexus.scala-tools.org/content/repositories/%s/".format(
    if (version.endsWith("-SNAPSHOT"))
      "snapshots"
    else
      "releases"
  )
}

object Dependencies {
  //BSON 
  //val bsonJava = "org.mongodb" % "bson" % "2.7.1"  // currently broken for just bson
  val bsonJava = "org.mongodb" % "mongo-java-driver" % "2.9.3" 
  // Connection Pooling
  val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"

  // Netty
  val netty = "org.jboss.netty" % "netty" % "3.2.6.Final"

  // Testing Deps
  val specs2 = "org.specs2" %% "specs2" % "1.12.3" % "test" 
  val junit = "junit" % "junit" % "4.7" % "test"
  val mongoJava = "org.mongodb" % "mongo-java-driver" % "2.8.0" % "test->default"
  val slf4j = "org.slf4j" % "slf4j-api" % "1.6.1"
  val slf4jJCL = "org.slf4j" % "slf4j-jcl" % "1.6.1"

  // Akka
  val akkaActors = "com.typesafe.akka" % "akka-actor" % "2.1.0"

  def scalaVersionString(scalaVer: sbt.SettingKey[String]): String = {
    var result = ""
    scalaVer { sv => result = sv }
    if (result == "") result = "2.8.1"
    result
  }

}

object Resolvers {

  val typesafeRepo = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/snapshots/"
  val sonaReleases = "sonatype releases" at "https://oss.sonatype.org/content/repositories/releases"
  val jbossRepo = "JBoss Public Repo" at "https://repository.jboss.org/nexus/content/groups/public-jboss/"
  val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
}
