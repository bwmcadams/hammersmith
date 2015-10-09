import Libraries._

val scalaVer = "2.11.7"

lazy val commonSettings = Seq(
  organization := "codes.bytes",
  version := "0.3.0-SNAPSHOT",
  scalaVersion := scalaVer,
  scalacOptions ++= compileOptions,
  parallelExecution in Test := false,
  libraryDependencies ++= Seq(
    netty,
    scalaLogging,
    specs2,
    junit,
    akkaActors,
    akkaTestkit
  )
)

lazy val hammersmith = (project in file(".")).
  settings(commonSettings: _*).
  settings(
      name := "hammersmith"
  ).
  aggregate(core)


lazy val core = (project in file("core")).
  settings(commonSettings).
  settings(
    name := "hammersmith-core",
    libraryDependencies ++= Seq(
      mongoJava
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
      akkaTestkit
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

