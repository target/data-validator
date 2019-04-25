package com.target.data_validator

import cats.syntax.functor._
import io.circe.{Decoder, Json}
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession

abstract class ValidatorOutput extends Substitutable with EventLog {
  def write(json: Json)(implicit spark: SparkSession): Boolean
  def substituteVariables(dict: VarSubstitution): ValidatorOutput
  def configCheck(spark: SparkSession): Boolean
}

case class PipeOutput(pipe: String, ignoreError: Option[Boolean]) extends ValidatorOutput {

  override def write(json: Json)(implicit spark: SparkSession): Boolean = {
    logger.info(s"Piping json output to '$pipe' ignoreError: ${ignoreError.getOrElse(false)}")
    val timer = new ValidatorTimer(s"PipeOutput($pipe)")
    addEvent(timer)

    val (fail, out, err) = timer.time(IO.writeStringToPipe(pipe, json.noSpaces))

    if (fail) {
      logger.error(s"Program `$pipe` failed!")
      if (out.isEmpty) {
        logger.error("stdout empty!")
      } else {
        out.foreach(o => logger.error(s"stdout: $o"))
      }
      if (err.isEmpty) {
        logger.error("stderr empty!")
      } else {
        err.foreach(o => logger.error(s"stderr: $o"))
      }
      !ignoreError.getOrElse(false)
    } else {
      false
    }
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorOutput =
    this.copy(getVarSub(pipe, "pipe", dict))

  override def configCheck(spark: SparkSession): Boolean = {
    val ret = IO.canExecute(pipe.split("\\s").head)(spark)
    if (!ret) {
      val msg = s"Pipe:'$pipe' not executable!"
      validatorError(msg)
    }
    !ret
  }
}

case class FileOutput(filename: String, append: Option[Boolean]) extends ValidatorOutput {

  override def write(json: Json)(implicit spark: SparkSession): Boolean = {
    logger.info(s"Writing json output to file '$filename append: ${append.getOrElse(false)}")
    val timer = new ValidatorTimer(s"FileOutput($filename)")
    timer.time(IO.writeJSON(filename, json, append.getOrElse(false)))
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorOutput =
    this.copy(getVarSub(filename, "filename", dict))
  override def configCheck(spark: SparkSession): Boolean = {
    val ret = IO.canAppendOrCreate(filename, append.getOrElse(false))(spark)
    if (!ret) {
      val msg = s"FileOutput '$filename' append: $append cannot write or append!"
      logger.error(msg)
      validatorError(msg)
    }
    !ret
  }
}

object ValidatorOutput {
  implicit val decodeOutputs: Decoder[ValidatorOutput] = List[Decoder[ValidatorOutput]](
    Decoder[PipeOutput].widen,
    Decoder[FileOutput].widen
  ).reduce(_ or _)
}
