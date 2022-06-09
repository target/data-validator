val circeVersion = "0.10.0"

val scala211 = "2.11.12"
val scala212 = "2.12.15"
val scala213 = "2.13.8"

ThisBuild / organization := "com.target"
enablePlugins(GitVersioning)
ThisBuild / git.useGitDescribe := true
ThisBuild / versionScheme := Some("early-semver")

// sbt auto-reload on changes
Global / onChangedBuildSource := ReloadOnSourceChanges

// Enforces scalastyle checks
val compileScalastyle = TaskKey[Unit]("compileScalastyle")
val generateTestData = TaskKey[Unit]("generateTestData")

/////////////
// Publishing
/////////////
githubOwner := "target"
githubRepository := "data-validator"
// this unfortunately must be set strangely because GitHub requires a token for pulling packages
// and sbt-github-packages does not allow the user to configure the resolver not to be used.
// https://github.com/djspiewak/sbt-github-packages/issues/28
githubTokenSource := (TokenSource.Environment("GITHUB_TOKEN") ||
  TokenSource.GitConfig("github.token") ||
  TokenSource.Environment("SHELL")) // it's safe to assume this exists and is not unique
publishTo := githubPublishTo.value

lazy val commonSettings: SettingsDefinition = Def.settings(
  name := "data-validator",
  buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
  buildInfoPackage := "com.target.data_validator",
  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "com.github.scopt" %% "scopt" % "3.7.0",
    "com.sun.mail" % "javax.mail" % "1.6.2",
    "com.lihaoyi" %% "scalatags" % "0.6.7",
    "io.circe" %% "circe-yaml" % "0.9.0",
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    // "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "junit" % "junit" % "4.12" % Test,
    "com.novocode" % "junit-interface" % "0.11" % Test exclude ("junit", "junit-dep")
  ),
  (Test / fork) := true,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  (Test / parallelExecution) := false,

  // required for unit tests, but not set in some environments
  (Test / envVars) ++= Map(
    "JAVA_HOME" ->
      Option(System.getenv("JAVA_HOME"))
        .getOrElse(System.getProperty("java.home"))
  ),
  (assembly / mainClass) := Some("com.target.data_validator.Main"),
  scalastyleFailOnWarning := true,
  scalastyleFailOnError := true,
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  (Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value,
  (Compile / run) := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,

  /////////////
  // Publishing
  /////////////
  githubOwner := "target",
  githubRepository := "data-validator",
// this unfortunately must be set strangely because GitHub requires a token for pulling packages
// and sbt-github-packages does not allow the user to configure the resolver not to be used.
// https://github.com/djspiewak/sbt-github-packages/issues/28
  githubTokenSource := (TokenSource.Environment("GITHUB_TOKEN") ||
    TokenSource.GitConfig("github.token") ||
    TokenSource.Environment("SHELL")), // it's safe to assume this exists and is not unique
  publishTo := githubPublishTo.value
)

lazy val root = (projectMatrix in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .jvmPlatform(
    scalaVersions = Seq(scala211),
    settings = Seq(
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4" % Provided,
      (Compile / runMain) := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated,
      generateTestData := {
        (Compile / runMain).toTask(" com.target.data_validator.GenTestData").value
      }
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(scala212),
    settings = Seq(libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % Provided)
  )
  .jvmPlatform(
    scalaVersions = Seq(scala213),
    settings = Seq(libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided)
  )
