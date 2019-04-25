package com.target.data_validator

import java.net.InetAddress

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.Try
import scalatags.Text.all._

case class ValidatorConfig(
  numKeyCols: Int,
  numErrorsToReport: Int,
  email: Option[EmailConfig],
  detailedErrors: Boolean = true,
  vars: Option[List[ConfigVar]],
  outputs: Option[List[ValidatorOutput]],
  tables: List[ValidatorTable]
) extends LazyLogging {

  def failed: Boolean = tables.exists(_.failed)

  def checkOutputs(session: SparkSession): Boolean = outputs match {
    case Some(outs) => outs.map(_.configCheck(session)).exists(x => x)
    case None => false
  }

  def checkTables(session: SparkSession, dict: VarSubstitution): Boolean = {
    val error = tables.map(_.configCheck(session, dict)).exists(x => x)
    if (error) {
      logger.error("checkTables failed!")
    }
    error
  }

  def configCheck(session: SparkSession, dict: VarSubstitution): Boolean = {
    val outputsError = checkOutputs(session)
    val tableError = checkTables(session, dict)
    if (outputsError || tableError) {
      logger.error("configCheck failed!")
    }
    outputsError || tableError
  }

  def quickChecks(session: SparkSession, dict: VarSubstitution): Boolean = {
    tables.map(_.quickChecks(session, dict)(this)).exists(x => x)
  }

  def generateHTMLReport(): Tag = html(h1("Validator Report"), hr(), tables.map(_.generateHTMLReport()))

  def substituteVariables(varSub: VarSubstitution): Option[ValidatorConfig] = {
    logger.info("substituteVariables()")
    Some(this.copy(email = this.email.map(_.substituteVariables(varSub)),
      tables = this.tables.map(_.substituteVariables(varSub)),
      outputs = this.outputs.map(_.map(_.substituteVariables(varSub)))))
  }

  def genJsonReport(varSub: VarSubstitution)(implicit spark: SparkSession): Json = {
    import JsonEncoders._

    Json.obj(
      ("numKeyCols", numKeyCols.asJson),
      ("numErrorsToReport", numErrorsToReport.asJson),
      ("email", email.asJson),
      ("detailedErrors", detailedErrors.asJson),
      ("vars", vars.asJson),
      ("varSubDict", varSub.dict.asJson),
      ("failed", failed.asJson),
      ("buildInfo", ValidatorConfig.buildInfoJson),
      ("runtimeInfo", ValidatorConfig.runtimeInfoJson(spark)),
      ("outputs", outputs.asJson),
      ("tables", tables.asJson),
      ("events", EventLog.events.asJson)
    )
  }
}

object ValidatorConfig {
  private def buildInfoJson: Json = Json.obj(
    ("name", Json.fromString(BuildInfo.name)),
    ("version", Json.fromString(BuildInfo.version)),
    ("scalaVersion", Json.fromString(BuildInfo.scalaVersion)),
    ("sbtVersion", Json.fromString(BuildInfo.sbtVersion)),
    ("sparkVersion", Json.fromString(org.apache.spark.SPARK_VERSION)),
    ("javaVersion", Json.fromString(System.getProperty("java.version")))
  )

  private def propsToJson: Json = {
    val props = System.getProperties.asScala.toList.map(x => (x._1, Json.fromString(x._2)))
    Json.obj(props: _*)
  }

  private def envToJson: Json = {
    val env = System.getenv.asScala.toList.map(x => (x._1, Json.fromString(x._2)))
    Json.obj(env: _*)
  }

  private def runtimeInfoJson(spark: SparkSession): Json = {
    val startTimeMs = spark.sparkContext.startTime
    val endTimeMs = System.currentTimeMillis()
    val durationMs = endTimeMs - startTimeMs
    Json.obj(
      ("hostname", Json.fromString(Try(InetAddress.getLocalHost.getHostName).getOrElse("UNKNOWN"))),
      ("applicationId", Json.fromString(spark.sparkContext.applicationId)),
      ("sparkUser", Json.fromString(spark.sparkContext.sparkUser)),
      ("startTimeMs", Json.fromLong(startTimeMs)),
      ("endTimeMs", Json.fromLong(endTimeMs)),
      ("durationMs", Json.fromLong(durationMs)),
      ("properties", propsToJson),
      ("environment", envToJson)
    )
  }
}
