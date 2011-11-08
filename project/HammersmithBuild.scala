import sbt._
import Keys._
import com.typesafe.sbtscalariform.ScalariformPlugin
import ScalariformPlugin.{ format, formatPreferences }

object HammersmithBuild extends Build {
  import Dependencies._
  import Resolvers._

  lazy val buildSettings = Seq(
    organization := "com.mongodb.async",
    version := "0.2.9-1",
    scalaVersion := "2.9.1",
    crossScalaVersions := Seq("2.9.1", "2.9.0-1")
  )

  /**
   * Import some sample data for testing
   */
  "mongoimport -d hammersmithIntegration -c yield_historical.in --drop ./mongo-driver/src/test/resources/yield_historical_in.json" !

  "mongoimport -d hammersmithIntegration -c books --drop ./mongo-driver/src/test/resources/bookstore.json" ! 

  override lazy val settings = super.settings ++ buildSettings

  lazy val baseSettings = Defaults.defaultSettings  ++ formatSettings

  lazy val parentSettings = baseSettings ++ Publish.settings

  lazy val formatSettings = ScalariformPlugin.settings ++ Seq(
    formatPreferences in Compile := formattingPreferences,
    formatPreferences in Test    := formattingPreferences
  )

  def formattingPreferences = {
    import scalariform.formatter.preferences._
    FormattingPreferences().setPreference(AlignParameters, true).
                            setPreference(DoubleIndentClassDeclaration, true).
                            setPreference(IndentLocalDefs, true).
                            setPreference(PreserveDanglingCloseParenthesis, true).
                            setPreference(RewriteArrowSymbols, true)
  }


  lazy val defaultSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq(commonsPool, scalaj_collection, netty, twitterUtilCore, slf4j, specs2),
    resolvers ++= Seq(sonaReleases, jbossRepo, sbtReleases, sbtSnapshots, twttrRepo),
    autoCompilerPlugins := true,
    parallelExecution in Test := true,
    testFrameworks += TestFrameworks.Specs2
  ) 

  lazy val hammersmith = Project(
    id = "hammersmith",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(bson, mongo)
  )

  lazy val bson = Project(
    id = "bson-driver",
    base = file("bson-driver"),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= Seq(bsonJava, mongoJava)
    )
  )

  lazy val mongo = Project(
    id = "mongo-driver",
    base = file("mongo-driver"),
    settings = defaultSettings ++ Seq(
      libraryDependencies += slf4jJCL
    )
  ) dependsOn(bson)


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
  val bsonJava = "org.mongodb" % "mongo-java-driver" % "2.7.1" 
  // Connection Pooling
  val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"

  val scalaj_collection = "org.scalaj" %% "scalaj-collection" % "1.2"
  // Netty
  val netty = "org.jboss.netty" % "netty" % "3.2.6.Final"

  // Twitter-util
  val twitterUtilCore = "com.twitter" % "util-core" % "1.12.2"

  // Testing Deps
  val specs2 = "org.specs2" %% "specs2" % "1.5" % "provided" 
  val mongoJava = "org.mongodb" % "mongo-java-driver" % "2.7.1" % "test->default"
  val slf4j = "org.slf4j" % "slf4j-api" % "1.6.1"
  val slf4jJCL = "org.slf4j" % "slf4j-jcl" % "1.6.1" % "test"

  def specs2ScalazCore(scalaVer: sbt.SettingKey[String]) = 
    scalaVersionString(scalaVer) match {
      case "2.8.1" => "org.specs2" %% "specs2-scalaz-core" % "5.1-SNAPSHOT" % "test"
      case _ => "org.specs2" %% "specs2-scalaz-core" % "6.0.RC2" % "test"
    }

  def scalaVersionString(scalaVer: sbt.SettingKey[String]): String = {
    var result = ""
    scalaVer { sv => result = sv }
    if (result == "") result = "2.8.1"
    result
  }

}

object Resolvers {
  val sbtSnapshots = "snapshots" at "http://scala-tools.org/repo-snapshots"
  val sbtReleases  = "releases" at "http://scala-tools.org/repo-releases"

  val sonaReleases = "releases" at "https://oss.sonatype.org/content/repositories/releases"
  val jbossRepo = "JBoss Public Repo" at "https://repository.jboss.org/nexus/content/groups/public-jboss/"
  val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
}
