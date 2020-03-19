package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorCounter, ValidatorError, VarSubstitution}
import io.circe._
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

case class SumOfNumericColumnCheck(
  column: String,
  thresholdType: String,
  threshold: Option[Json] = None,
  lowerBound: Option[Json] = None,
  upperBound: Option[Json] = None,
  inclusive: Option[Boolean] = None
)
  extends ColumnBased(column, Sum(UnresolvedAttribute(column)).toAggregateExpression()) {

  override def name: String = "SumOfNumericColumn"

  def boundsAreInclusive: Boolean = inclusive.getOrElse(false)

  private def createTypedLiteral(json: Json)(implicit dataType: DataType): Expression = {
    ValidatorBase.createLiteral(dataType, json)
  }

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    implicit val dataType: DataType = r.schema(idx).dataType
    val rawValueForLogging = r.get(idx)

    val rowValueAsExpr: Expression = Literal.create(rawValueForLogging, dataType)
    // by the time this is executed, the options have been verified
    lazy val thresholdAsExpr: Expression = createTypedLiteral(threshold.get)
    lazy val lowerBoundAsExpr: Expression = createTypedLiteral(lowerBound.get)
    lazy val upperBoundAsExpr: Expression = createTypedLiteral(upperBound.get)

    val failedIfFalseExpr = thresholdType match {
      case "over" if threshold.isDefined =>
        if (boundsAreInclusive) {
          GreaterThanOrEqual(rowValueAsExpr, thresholdAsExpr)
        } else {
          GreaterThan(rowValueAsExpr, thresholdAsExpr)
        }
      case "under" if threshold.isDefined =>
        if (boundsAreInclusive) {
          LessThanOrEqual(rowValueAsExpr, thresholdAsExpr)
        } else {
          LessThan(rowValueAsExpr, thresholdAsExpr)
        }
      case "between" if lowerBound.isDefined && upperBound.isDefined =>
        if (boundsAreInclusive) {
          And(GreaterThanOrEqual(rowValueAsExpr, lowerBoundAsExpr), LessThanOrEqual(rowValueAsExpr, upperBoundAsExpr))
        } else {
          And(GreaterThan(rowValueAsExpr, lowerBoundAsExpr), LessThan(rowValueAsExpr, upperBoundAsExpr))
        }
      case "outside" if lowerBound.isDefined && upperBound.isDefined =>
        if (boundsAreInclusive) {
          Or(LessThanOrEqual(rowValueAsExpr, lowerBoundAsExpr), GreaterThanOrEqual(rowValueAsExpr, upperBoundAsExpr))
        } else {
          Or(LessThan(rowValueAsExpr, lowerBoundAsExpr), GreaterThan(rowValueAsExpr, upperBoundAsExpr))
        }
      case _ =>
        val msg = s"""
                     |Unknown threshold type $thresholdType or one of the following is required and not present:
                     |threshold: $threshold (required for over/under)
                     |lowerBound: $lowerBound upperBound: $upperBound (required for between/outside)
              """.stripMargin
        logger.error(msg)
        addEvent(ValidatorError(msg))
        Literal.FalseLiteral
    }

    val failedIfFalse = failedIfFalseExpr.eval()
    failed = !failedIfFalse.asInstanceOf[Boolean]
    addEvent(ValidatorCounter("rowCount", count))
    addEvent(ValidatorCheckEvent(failed, s"$name $thresholdType on $column: [$failedIfFalseExpr]", count, 1))
    failed
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = copy(
      column = getVarSub(column, "column", dict),
      thresholdType = getVarSub(thresholdType, "thresholdType", dict),
      threshold = threshold.map(getVarSubJson(_, "threshold", dict)),
      lowerBound = lowerBound.map(getVarSubJson(_, "lowerBound", dict)),
      upperBound = upperBound.map(getVarSubJson(_, "upperBound", dict))
    )
    this.getEvents.foreach(ret.addEvent)
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
    failed
  }

  override def toJson: Json = {
    val additionalFieldsForReport = Json.fromFields(Set(
      "type" -> Json.fromString("sumOfNumericColumn"),
      "failed" -> Json.fromBoolean(failed)
    ))

    val base = SumOfNumericColumnCheck.encoder(this)
    base.deepMerge(additionalFieldsForReport)
  }
}

object SumOfNumericColumnCheck {
  val encoder: Encoder[SumOfNumericColumnCheck] = deriveEncoder[SumOfNumericColumnCheck]
  val decoder: Decoder[SumOfNumericColumnCheck] = deriveDecoder[SumOfNumericColumnCheck]
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = decoder.apply(c)
}
