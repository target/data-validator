package com.target.data_validator

import cats.syntax.functor._
import com.target.data_validator.EnvironmentVariables.{Error, Inaccessible, Present, Unset}
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, Json}
import io.circe.generic.auto._
import org.apache.spark.sql.SparkSession

import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Failure, Success, Try}

sealed trait ConfigVar extends EventLog with Substitutable {
  def addEntry(spark: SparkSession, varSub: VarSubstitution): Boolean
}

case class NameValue(name: String, value: Json) extends ConfigVar {
  override def addEntry(spark: SparkSession, varSub: VarSubstitution): Boolean = {
    logger.debug(s"name: $name value: ${value.noSpaces}")
    varSub.add(name, getVarSubJson(value, name, varSub))
  }
}

case class NameEnv(name: String, env: String) extends ConfigVar {

  override def addEntry(spark: SparkSession, varSub: VarSubstitution): Boolean = {
    val newEnv = getVarSub(env, name, varSub)
    EnvironmentVariables.get(newEnv) match {
      case Inaccessible(message) => logger.error(message); true
      case Error(message) => logger.error(message); true
      case Unset => {
        val msg = s"Variable '$name' cannot be processed env variable '$newEnv' not found!"
        logger.error(msg)
        addEvent(ValidatorError(msg))
        true
      }
      case Present(value) => {
        val resolvedEnvVar = getVarSubJson(JsonUtils.string2Json(value), name, varSub)
        logger.debug(s"name: $name env: $env getEnv: $value resolvedEnvVar: $resolvedEnvVar")
        varSub.add(name, resolvedEnvVar)
      }
    }
  }
}

case class NameShell(name: String, shell: String) extends ConfigVar {
  override def addEntry(spark: SparkSession, varSub: VarSubstitution): Boolean = {
    val newShell = getVarSub(shell, "shell", varSub)
    val timer = new ValidatorTimer(s"NameShell($name, $newShell)")
    val out = new StringBuilder
    val err = new StringBuilder
    val processLogger = ProcessLogger(out append _, err append _)
    addEvent(timer)
    timer.time {
      Try(Process(Seq("/bin/sh", "-c", newShell)) ! processLogger) match {
        case Failure(exception) =>
          validatorError(s"NameShell($name, $newShell) Failed with exception $exception"); true
        case Success(exitCode) if exitCode != 0 =>
          validatorError(s"NameShell($name, $newShell) Ran but returned exitCode: $exitCode stderr: ${err.toString()}")
          true
        case Success(0) if out.isEmpty =>
          validatorError(s"NameShell($name, $newShell) Ran but returned No output")
          true
        case Success(0) =>
          val value = out.toString.split("\n").head
          logger.debug(s"name: $name shell: $newShell output: $value")
          varSub.add(name, getVarSubJson(JsonUtils.string2Json(value), name, varSub)); false
      }
    }
  }
}

case class NameSql(name: String, sql: String) extends ConfigVar {
  override def addEntry(spark: SparkSession, varSub: VarSubstitution): Boolean = {
    val timer = new ValidatorTimer(s"NameSql($name, $sql)")
    addEvent(timer)
    timer.time {
      Try(spark.sql(getVarSub(sql, name, varSub)).head(1)) match {
        case Failure(exception) =>
          validatorError(s"NameSql($name, $sql) Failed with exception $exception")
          true
        case Success(rows) if rows.isEmpty =>
          validatorError(s"NameSql($name, $sql) Returned 0 rows.")
          true
        case Success(rows) =>
          val json = JsonUtils.row2Json(rows.head, 0)
          logger.debug(s"name: $name sql: $sql result: ${rows.head} json: ${JsonUtils.debugJson(json)}")
          varSub.add(name, json)
      }
    }
  }
}

object ConfigVar extends LazyLogging {

  implicit val decodeConfigVar: Decoder[ConfigVar] = List[Decoder[ConfigVar]](
    Decoder[NameValue].widen,
    Decoder[NameShell].widen,
    Decoder[NameEnv].widen,
    Decoder[NameSql].widen
  ).reduceLeft(_ or _)

}
