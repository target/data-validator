package com.target.data_validator

import com.target.data_validator.Main.CmdLineOptions
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object Reports extends LazyLogging with EventLog {

  def emailReport(
    mainConfig: CmdLineOptions,
    config: ValidatorConfig,
    varSub: VarSubstitution
  )(implicit spark: SparkSession): Unit = {
    if (mainConfig.htmlReport.isDefined || config.email.isDefined) {
      val htmlReport = config.generateHTMLReport()

      mainConfig.htmlReport.foreach { htmlFilename =>
        logger.info(s"Writing HTML report to $htmlFilename")
        IO.writeHTML(htmlFilename, htmlReport)
      }

      config.email.foreach { emailConfig =>
        logger.info(s"Sending email report emailConfig: $emailConfig")
        Emailer.sendHtmlMessage(emailConfig, htmlReport.render)
      }
    }
  }

  def jsonReport(
    mainConfig: CmdLineOptions,
    config: ValidatorConfig,
    varSub: VarSubstitution
  )(implicit spark: SparkSession): Unit = {
    if (config.outputs.isDefined || mainConfig.jsonReport.isDefined) {
      val jsonReport = config.genJsonReport(varSub)
      mainConfig.jsonReport.foreach { jsonFilename =>
        logger.info(s"Writing JSON report to $jsonFilename")
        IO.writeJSON(jsonFilename, jsonReport, append = true)
      }

      for (outputs <- config.outputs; out <- outputs) {
        if (out.write(jsonReport)) {
          val msg = s"ERROR: Failed to write out: $out"
          logger.error(msg)
          validatorError(msg)
        }
      }
    }
  }

}
