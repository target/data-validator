package com.target.data_validator

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

trait Substitutable extends LazyLogging with EventGenerator {
  def getVarSub(v: String, field: String, dict: VarSubstitution): String =
    dict.replaceVars(v) match {
      case Left(newV) =>
        if (v != newV) {
          logger.info(s"Substituting $field var: $v with `$newV`")
          addEvent(VarSubEvent(v, newV))
        }
        newV
      case Right(event) =>
        addEvent(event)
        logger.warn(s"Field: $field msg: $event")
        v
    }

  def getVarSubJson(j: Json, field: String, dict: VarSubstitution): Json =
    dict.replaceJsonVars(j) match {
      case Left(newJ) =>
        if (j != newJ) {
          logger.info(s"Substituting Json $field Json: $j with `$newJ`")
          addEvent(VarSubJsonEvent(j.toString(), newJ))
        }
        newJ
      case Right(event) =>
        addEvent(event)
        logger.warn(s"Field: $field msg: $event")
        j
    }
}
