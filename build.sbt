import Libraries._

val scalaVer = "2.11.7"

lazy val commonSettings = Seq(
  organization := "codes.bytes",
  version := "0.3.0-SNAPSHOT",
  scalaVersion := scalaVer,
  scalacOptions ++= compileOptions,
  parallelExecution in Test := false,
  fork in Test := true,
  libraryDependencies ++= Seq(
    netty,
    scalaLogging,
    specs2,
    junit,
    akkaActors,
    akkaTestkit,
    logback,
    // used for saner logback config because fuck XML. TODO: Make this optional for users...
    groovy
  )
)

lazy val hammersmith = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    parallelExecution in Test := false,
    name := "hammersmith"
  ).
  aggregate(core, akka)


lazy val core = (project in file("core")).
  settings(commonSettings).
  settings(
    name := "hammersmith-core",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
    )
  )

/**
 * Still TBD if we'll have one single "akka" project
 * or a separate one for raw/low level Akka IO and Akka Streams
 */
lazy val akka = (project in file("akka")).
  settings(commonSettings).
  settings(
    name := "hammersmith-akka",
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      akkaActors,
      akkaTestkit,
      mongoJava
    )
  ).
  dependsOn(core)

lazy val compileOptions = Seq(
  "-unchecked",
  "-deprecation",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

