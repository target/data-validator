name := "data-validator"
organization := "com.target"

val sparkVersion = settingKey[String]("Spark version")

sparkVersion := System.getProperty("sparkVersion", "2.3.4")

scalaVersion := {
  if (sparkVersion.value > "3.0") {
    "2.12.13"
  } else {
    "2.11.12"
  }
}

val sparkValidationVersion = settingKey[String]("Version of package")

sparkValidationVersion := "0.15.0"

version := sparkVersion.value + "_" + sparkValidationVersion.value

val circeVersion = settingKey[String]("Circe version")
val circeYamlVersion = settingKey[String]("Circe YAML version")

circeVersion := {
  if (sparkVersion.value > "3.0") {
    "0.14.2"
  } else {
    "0.11.2"
  }
}

circeYamlVersion := {
  if (sparkVersion.value > "3.0") {
    "0.14.2"
  } else {
    "0.10.1"
  }
}

//addDependencyTreePlugin
enablePlugins(GitVersioning)
git.useGitDescribe := true
ThisBuild / versionScheme := Some("early-semver")

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

enablePlugins(BuildInfoPlugin)
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.target.data_validator"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "com.github.scopt" %% "scopt" % "4.1.0",
  "com.sun.mail" % "javax.mail" % "1.6.2",
  "com.lihaoyi" %% "scalatags" % "0.12.0",
  "io.circe" %% "circe-yaml" % circeYamlVersion.value,
  "io.circe" %% "circe-core" % circeVersion.value,
  "io.circe" %% "circe-generic" % circeVersion.value,
  "io.circe" %% "circe-parser" % circeVersion.value,
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided,
  "junit" % "junit" % "4.13.2" % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test exclude ("junit", "junit-dep")
)

Test / fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
Test / parallelExecution := false
// required for unit tests, but not set in some environments
Test / envVars ++= Map(
  "JAVA_HOME" ->
    Option(System.getenv("JAVA_HOME"))
      .getOrElse(System.getProperty("java.home"))
)

assembly / mainClass := Some("com.target.data_validator.Main")

// Enforces scalastyle checks
val compileScalastyle = TaskKey[Unit]("compileScalastyle")
scalastyleFailOnWarning := true
scalastyleFailOnError := true

compileScalastyle := (Compile / scalastyle).toTask("").value
(Compile / compile) := ((Compile / compile) dependsOn compileScalastyle).value

(Compile / run) := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

(Compile / runMain) := Defaults.runMainTask(Compile / fullClasspath, Compile / run / runner).evaluated
TaskKey[Unit]("generateTestData") := {
  libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value
  (Compile / runMain).toTask(" com.target.data_validator.GenTestData").value
}
