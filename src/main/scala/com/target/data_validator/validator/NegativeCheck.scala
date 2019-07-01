package com.target.data_validator.validator

import com.target.data_validator.{ValidatorError, VarSubstitution}
import com.target.data_validator.JsonEncoders.eventEncoder
import com.target.data_validator.validator.ValidatorBase.I0
import com.typesafe.scalalogging.LazyLogging
import io.circe.{DecodingFailure, HCursor, Json}
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, LessThan}
import org.apache.spark.sql.types.{NumericType, StructType}

case class NegativeCheck(column: String, threshold: Option[String]) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = NegativeCheck(getVarSub(column, "column", dict),
      threshold.map(getVarSub(_, "threshold", dict)))
    getEvents.foreach(ret.addEvent)
    ret
  }

  override def configCheck(df: DataFrame): Boolean = {
    findColumnInDataFrame(df, column) match {
      case Some(ft) if ft.dataType.isInstanceOf[NumericType] => Unit
      case Some(ft) =>
        val msg = s"Column: $column found, but not of numericType type: ${ft.dataType}"
        logger.error(msg)
        addEvent(ValidatorError(msg))
      case None =>
        val msg = s"Column: $column not found in schema."
        logger.error(msg)
        addEvent(ValidatorError(msg))
    }
    configCheckThreshold
    failed
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression =
    LessThan (UnresolvedAttribute(column), I0)

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("negativeCheck")),
    ("column", Json.fromString(column)),
    ("threshold", Json.fromString(threshold.getOrElse("0"))),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )
}

object NegativeCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val threshold = c.downField("threshold").as[String].right.toOption

    logger.debug(s"Parsing NegativeCheck(column:$column, threshold:$threshold) config.")
    scala.util.Right(NegativeCheck(column, threshold))
  }
}
