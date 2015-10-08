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
    akkaActors,
    akkaTestkit,
    specs2,
    junit
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

lazy val compileOptions = Seq(
  "-unchecked",
  "-deprecation",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

