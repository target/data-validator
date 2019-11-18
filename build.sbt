name := "data-validator"
organization := "com.target"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

val circeVersion = "0.10.0"

enablePlugins(GitVersioning)
git.useGitDescribe := true

val artifactoryUrl:Option[java.net.URL] = sys.env.get("ARTIFACTORY_URL").map(new java.net.URL(_))

// Publish info
publishTo := artifactoryUrl.map(url =>"Artifactory Realm" at url.toString)

credentials ++= (
  for {
    artifactoryUsername <- sys.env.get("ARTIFACTORY_USERNAME")
    artifactoryPassword <- sys.env.get("ARTIFACTORY_PASSWORD")
    url <- artifactoryUrl
  } yield Credentials(
    "Artifactory Realm",
    url.getHost,
    artifactoryUsername,
    artifactoryPassword
  )
).toSeq

enablePlugins(BuildInfoPlugin)
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.target.data_validator"

resolvers += "Hortonworks Repo" at "https://repo.hortonworks.com/content/repositories/releases"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "com.sun.mail" % "javax.mail" % "1.6.2",
  "com.lihaoyi" %% "scalatags" % "0.6.7",
  "io.circe" %% "circe-yaml" % "0.9.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.1.0.53-1" % Provided,

  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test exclude("junit", "junit-dep")
)

def fixTestClasspath(cp: Seq[Attributed[File]]): Seq[Attributed[File]] = {
  val jars = "hive-warehouse-connector_2.11-1.0.0.3.1.0.53-1.jar"
  val (seq1, seq2) = cp.partition(c => c.data.getName.indexOf(jars) < 0)
  seq1 ++ seq2
}

dependencyClasspath in Test ~= fixTestClasspath

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

mainClass in assembly := Some("com.target.data_validator.Main")

// Enforces scalastyle checks
val compileScalastyle = TaskKey[Unit]("compileScalastyle")
scalastyleFailOnWarning := true
scalastyleFailOnError := true
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value
