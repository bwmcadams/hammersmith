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
    scalaLogging,
    specs2,
    logback,
    // used for saner logback config because fuck XML. Used for test config only.
    groovy
  )
)

lazy val hammersmith = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "hammersmith"
  ).
  aggregate(core, akka, bson)


lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "hammersmith-core",
    libraryDependencies ++= Seq(
    )
  ).
  dependsOn(bson)

/**
 * The most likely scenario is we will base *all* bson ser/deser on scodec,
 * given the bridges to other common apis like Akka.IO. Let's do it once, let's do it right.
 * Let's make it not have dependencies on other parts of the system so people can use it on their own.
 * We will have separate modules however for scalaz, akka support. I don't wanna pull in scalaz
 * for people who don't need it (however, spire support gives some advantages)
 */
lazy val bson = (project in file("bson")).
  settings(commonSettings: _*).
  settings(
    name := "hammersmith-bson",
    libraryDependencies ++= Seq(
      scodecCore,
      scodecBits,
      scodecSpire
    )
  )

/**
 * The core collections library. Contains the base interfaces, and
 * an immutable implementation.
 */
lazy val collections = (project in file("collections")).
  settings(commonSettings: _*).
  settings(
    name := "hammersmith-collections",
    libraryDependencies ++= Seq(
    )
  ).
  dependsOn(core)


/**
 * A mutable collections library for Hammersmith, *purely optional*.
 *
 * We shouldn't encourage mutable by default, so you opt in to mutable
 * collection support
 */
lazy val mutable_collections = (project in file("mutable-collections")).
  settings(commonSettings: _*).
  settings(
    name := "hammersmith-mutable-collections",
    libraryDependencies ++= Seq(
    )
  ).
  dependsOn(collections)

/**
 * Still TBD if we'll have one single "akka" project
 * or a separate one for raw/low level Akka IO and Akka Streams
 */
lazy val akka = (project in file("akka")).
  settings(commonSettings: _*).
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

