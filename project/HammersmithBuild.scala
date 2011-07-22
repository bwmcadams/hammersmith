import sbt._
import Keys._

object HammersmithBuild extends Build {
  import Dependencies._
  import Resolvers._

  lazy val buildSettings = Seq(
    organization := "com.mongodb.async",
    version := "0.2.8-SNAPSHOT",
    scalaVersion := "2.9.0-1"
  )

  /**
   * Import some sample data for testing
   */
  "mongoimport -d hammersmithIntegration -c yield_historical.in --drop ./mongo-driver/src/test/resources/yield_historical_in.json" !

  "mongoimport -d hammersmithIntegration -c books --drop ./mongo-driver/src/test/resources/bookstore.json" ! 

  override lazy val settings = super.settings ++ buildSettings

  lazy val baseSettings = Defaults.defaultSettings 

  lazy val parentSettings = baseSettings ++ Publish.settings

  lazy val defaultSettings = baseSettings ++ Seq(
    libraryDependencies ++= Seq(casbah, commonsPool, scalaj_collection, netty, twitterUtilCore, slf4j, specs2),
    resolvers ++= Seq(jbossRepo, sbtReleases, sbtSnapshots, twttrRepo, akkaRepo),
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
      libraryDependencies += mongoJava
    )
  )

  lazy val mongo = Project(
    id = "mongo-driver",
    base = file("mongo-driver"),
    settings = defaultSettings ++ Seq(
      libraryDependencies ++= Seq(slf4jJCL, akkaActor)
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
  val casbah = "com.mongodb.casbah" %% "casbah-util" % "2.2.0-SNAPSHOT"
  // Connection Pooling
  val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"

  val scalaj_collection = "org.scalaj" %% "scalaj-collection" % "1.1"
  // Netty
  val netty = "org.jboss.netty" % "netty" % "3.2.4.Final"

  // Twitter-util
  val twitterUtilCore = "com.twitter" % "util-core" % "1.8.15"

  // akka-actor
  val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.1"

  // Testing Deps
  val specs2 = "org.specs2" %% "specs2" % "1.4" % "test"
  val mongoJava = "org.mongodb" % "mongo-java-driver" % "2.6.3" % "test->default"
  val slf4j = "org.slf4j" % "slf4j-api" % "1.6.1"
  val slf4jJCL = "org.slf4j" % "slf4j-jcl" % "1.6.1" % "test"
}

object Resolvers {
  val sbtSnapshots = "snapshots" at "http://scala-tools.org/repo-snapshots"
  val sbtReleases  = "releases" at "http://scala-tools.org/repo-releases"

  val jbossRepo = "JBoss Public Repo" at "https://repository.jboss.org/nexus/content/groups/public-jboss/"
  val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"

  val akkaRepo = "Akka Repo" at "http://akka.io/repository"
}
