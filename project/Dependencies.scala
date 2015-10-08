import sbt._

object Version {
  val akka         = "2.2.1"
  val mongoJava    = "2.9.3" // only used for testing
  val junit        = "4.7"
  val specs2       = "1.12.3"
  val netty        = "3.2.6.Final"
  val logback      = "1.1.3"
  val slf4j        = "1.7.12"
  val ficus        = "1.1.2"
  val scalaTest    = "2.2.4"
}

object Libraries {
  // Netty
  val netty = "org.jboss.netty" % "netty" % Version.netty

  // Logging
  //val slf4j = "org.slf4j" % "slf4j-api" % Version.slf4j
  //val slf4jJCL = "org.slf4j" % "slf4j-jcl" % Version.slf4j
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

  // Config
  val ficus = "net.ceedubs" %% "ficus" % Version.ficus

  // Testing Deps
  val specs2 = "org.specs2" %% "specs2" % Version.specs2 % "test"
  val junit = "junit" % "junit" % Version.junit % "test"
  val mongoJava = "org.mongodb" % "mongo-java-driver" % Version.mongoJava % "test->default"
  val scalaTest = "org.scalatest" %% "scalatest" % "2.2.4" % "test"

  // Akka
  val akkaActors = "com.typesafe.akka" %% "akka-actor" % Version.akka
  val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % Version.akka % "test"
  val akkaSlf4J = "com.typesafe.akka" %% "akka-slf4j" % Version.akka

}

