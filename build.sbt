import Libraries._

val scalaVer = "2.11.7"

lazy val commonSettings = Seq(
  organization := "codes.bytes",
  version := "0.3.0-SNAPSHOT",
  scalaVersion := scalaVer,
  scalacOptions ++= compileOptions,
  parallelExecution in Test := false,
  fork in Test := true,
  testOptions in Test += Tests.Argument("sequential"),
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
    name := "hammersmith"
  ).
  aggregate(core, akka)


lazy val core = (project in file("core")).
  settings(commonSettings).
  settings(
    name := "hammersmith-core",
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
  "-target:jvm-1.8", // come on, Java 1.7 is EOL. I'm not supporting it. Upgrade your JDK ... or use mongo's official drivers.
  "-encoding", "UTF-8"
)

