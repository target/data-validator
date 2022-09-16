val scala211 = "2.11.12"
val scala212 = "2.12.15"
val scala213 = "2.13.8"

val sparkVersion = "2.3.4"

val circeVersion = "0.11.2"

//addDependencyTreePlugin
ThisBuild / organization := "com.target"
enablePlugins(GitVersioning)
ThisBuild / git.useGitDescribe := true
ThisBuild / versionScheme := Some("early-semver")

// sbt auto-reload on changes
Global / onChangedBuildSource := ReloadOnSourceChanges

// Enforces scalastyle checks
val compileScalastyle = TaskKey[Unit]("compileScalastyle")
val generateTestData = TaskKey[Unit]("generateTestData")

val circeVersion = SettingKey[String]("circeVersion")
val circeYamlVersion = SettingKey[String]("circeYamlVersion")

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
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    "com.github.scopt" %% "scopt" % "4.1.0",
    "com.sun.mail" % "javax.mail" % "1.6.2",
    "com.lihaoyi" %% "scalatags" % "0.11.1",
    "io.circe" %% "circe-yaml" % circeYamlVersion.value,
    "io.circe" %% "circe-core" % circeVersion.value,
    "io.circe" %% "circe-generic" % circeVersion.value,
    "io.circe" %% "circe-parser" % circeVersion.value,
    // "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.scalatest" %% "scalatest" % "3.2.13" % Test,
    "junit" % "junit" % "4.13.2" % Test,
    "com.novocode" % "junit-interface" % "0.13.3" % Test exclude ("junit", "junit-dep")
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
      circeVersion := "0.11.2",
      circeYamlVersion := "0.10.1",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.4" % Provided,
      (Compile / runMain) := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated,
      generateTestData := {
        (Compile / runMain).toTask(" com.target.data_validator.GenTestData").value
      }
    )
  )
  .jvmPlatform(
    scalaVersions = Seq(scala212),
    settings = Seq(
      circeVersion := "0.14.2",
      circeYamlVersion := "0.14.1",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8" % Provided
    )
  )
/* .jvmPlatform(
    scalaVersions = Seq(scala213),
    settings = Seq(
      circeVersion := "0.14.2",
      circeYamlVersion := "0.14.1",
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % Provided
    )
  ) */
