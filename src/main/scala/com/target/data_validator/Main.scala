package com.target.data_validator

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object Main extends LazyLogging with EventLog {

  def loadConfigRun(mainConfig: CliOptions): (Boolean, Boolean) =
    ConfigParser.parseFile(mainConfig.configFilename, mainConfig.vars) match {
      case Left(error) =>
        logger.error(s"Failed to parse config file '${mainConfig.configFilename}, $error")
        (true, false)
      case Right(validatorConfig) => runChecks(mainConfig, validatorConfig)
    }

  def resolveVariables(spark: SparkSession, mainConfig: CliOptions, config: ValidatorConfig,
                       varSub: VarSubstitution): Option[ValidatorConfig] = {
    varSub.addMap(mainConfig.vars)

    config.vars match {
      case None => config.substituteVariables(varSub)
      case Some(vars) => if (vars.map(_.addEntry(spark, varSub)).exists(x => x)) {
        validatorError("Failed to resolve config variables")
        None
      } else {
        config.substituteVariables(varSub)
      }
    }
  }

  private def checkFile(spark: SparkSession, filename: Option[String], append: Boolean): Boolean = {
    logger.info(s"filename: $filename append: $append")
    if (filename.isDefined) {
      logger.info(s"CheckFile $filename")
      val ret = filename.exists(!IO.canAppendOrCreate(_, append)(spark))
      logger.info(s"Checking file '${filename.get} append: $append failed: $ret")
      if (ret) {
        logger.error(s"Filename: ${filename.get} error!")
      }
      ret
    } else {
      false
    }
  }

  def checkCliOutputs(spark: SparkSession, mainConfig: CliOptions): Boolean = {
    logger.info(s"Checking Cli Outputs htmlReport: ${mainConfig.htmlReport} jsonReport: ${mainConfig.jsonReport}")
    checkFile(spark, mainConfig.htmlReport, append = false) ||
      checkFile(spark, mainConfig.jsonReport, append = true)
  }

  def checkConfig(
                   spark: SparkSession,
                   mainConfig: CliOptions,
                   config: ValidatorConfig,
                   varSub: VarSubstitution
  ): Boolean = checkCliOutputs(spark, mainConfig) || config.configCheck(spark, varSub)

  def runSparkChecks(
                      spark: SparkSession,
                      mainConfig: CliOptions,
                      config: ValidatorConfig,
                      varSub: VarSubstitution
  ): Boolean = {
    logger.info("Running sparkChecks")
    Seq(config.quickChecks(spark, varSub), config.costlyChecks(spark, varSub)).exists(x => x)
  }

  /*
    * There are 2 types of errors we return (fatal, validator_status)
    * If fatal, we need to System.exit(1)
    * Otherwise we print a message `VALIDATOR_STATUS=PASS|FAIL
   */
  def runChecks(mainConfig: CliOptions, origConfig: ValidatorConfig): (Boolean, Boolean) = {
    val varSub = new VarSubstitution

    implicit val spark: SparkSession = SparkSession.builder.appName("data-validator").enableHiveSupport().getOrCreate()

    if (mainConfig.verbose) {
      logger.info("Verbose Flag detected")
      logger.info(s"Original config: $origConfig")
      Logger.getRootLogger.setLevel(Level.DEBUG)
    }

    // Resolve config
    val (fatal, validator_fail) = resolveVariables(spark, mainConfig, origConfig, varSub).map {
      config =>
        val fatal = config.failed && checkConfig(spark, mainConfig, config, varSub)
        if (fatal) {
          (fatal, false)
        } else {
          // Result is true in case of validation failure, otherwise false.
          val validatorFail = runSparkChecks(spark, mainConfig, config, varSub)

          if (validatorFail || mainConfig.emailOnPass) {
            Reports.emailReport(mainConfig, config, varSub)
          }
          Reports.jsonReport(mainConfig, config, varSub)

          (fatal, validatorFail)
        }
    }.getOrElse((true, false))
    spark.stop()

    (fatal, validator_fail)
  }

  def configLogging(): Unit = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/log4j-dv-spark.properties"))
    // props.list(System.err)
    PropertyConfigurator.configure(props)
    logger.info("Logging configured!")
  }

  def main (args: Array[String]): Unit = {
    configLogging()

    val parser = CliOptionParser.parser

    logger.info("Data Validator")

    parser.parse(args, CliOptions()) match {
      case Some(cliConfig: CliOptions) =>
        val (fatal, validatorFail) = loadConfigRun(cliConfig)

        if (fatal || validatorFail) {
          logger.error("data-validator failed!")
          println("DATA_VALIDATOR_STATUS=FAIL") // scalastyle:ignore
        } else {
          logger.info("data-validator success!")
          println("DATA_VALIDATOR_STATUS=PASS") // scalastyle:ignore
        }

        if (fatal || (validatorFail && cliConfig.exitErrorOnFail)) {
          System.exit(-1)
        }
      case None =>
        logger.error("Failed to Parse Command line Options.")
        println("DATA_VALIDATOR_STATUS=FAIL") // scalastyle:ignore
        System.exit(-1)
    }
  }
}
