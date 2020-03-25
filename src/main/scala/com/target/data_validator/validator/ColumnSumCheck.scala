package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorCounter, ValidatorError, VarSubstitution}
import io.circe._
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

case class ColumnSumCheck(
  column: String,
  minValue: Option[Json] = None,
  maxValue: Option[Json] = None,
  inclusive: Option[Json] = None
)
  extends ColumnBased(column, Sum(UnresolvedAttribute(column)).toAggregateExpression())
    with MinMaxArgs {

  override def name: String = "columnSumCheck"

  def boundsAreInclusive: Boolean = inclusive.flatMap(_.asBoolean).getOrElse(false)

  private def createTypedLiteral(json: Json)(implicit dataType: DataType): Expression = {
    ValidatorBase.createLiteral(dataType, json)
  }

  private def combineExpressions(
    maxTest: => Option[Expression],
    minTest: => Option[Expression]): Expression = {
    (minTest, maxTest) match {
      // this first case is unreachable in practice because configCheck should fail before this is called
      case (None, None) =>
        val msg = "Both min and max tests were None in columnSumCheck. Were minValue and maxValue defined?"
        logger.error(msg)
        addEvent(ValidatorError(msg))
        Literal.FalseLiteral
      case (None, Some(maxOnly)) => maxOnly
      case (Some(minOnly), None) => minOnly
      case (Some(min), Some(max)) => And(min, max)
    }
  }

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    implicit val dataType: DataType = r.schema(idx).dataType
    val rawValueForLogging = r.get(idx)

    val rowValueAsExpr: Expression = Literal.create(rawValueForLogging, dataType)
    // by the time this is executed, the options have been verified
    lazy val minValueAsExpr: Option[Expression] = minValue.map(createTypedLiteral)
    lazy val maxValueAsExpr: Option[Expression] = maxValue.map(createTypedLiteral)

    val failedIfFalseExpr = if (boundsAreInclusive) {
      // use OrEqual here
      val maxTest = maxValueAsExpr.map(LessThanOrEqual(rowValueAsExpr, _))
      val minTest = minValueAsExpr.map(GreaterThanOrEqual(rowValueAsExpr, _))
      combineExpressions(maxTest, minTest)
    } else {
      // no OrEqual here
      val maxTest = maxValueAsExpr.map(LessThan(rowValueAsExpr, _))
      val minTest = minValueAsExpr.map(GreaterThan(rowValueAsExpr, _))
      combineExpressions(maxTest, minTest)
    }

    val failedIfFalse = failedIfFalseExpr.eval()
    failed = !failedIfFalse.asInstanceOf[Boolean]
    addEvent(ValidatorCounter("rowCount", count))
    addEvent(ValidatorCheckEvent(failed, s"$name on $column: [$failedIfFalseExpr]", count, 1))
    failed
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = copy(
      column = getVarSub(column, "column", dict),
      minValue = minValue.map(getVarSubJson(_, "minValue", dict)),
      maxValue = maxValue.map(getVarSubJson(_, "maxValue", dict)),
      inclusive = inclusive.map(getVarSubJson(_, "inclusive", dict))
    )
    this.getEvents.foreach(ret.addEvent)
    ret
  }

  override def configCheck(df: DataFrame): Boolean = {
    logger.debug(s"Full check config: ${this.toString}")
    checkValuesPresent()
    checkInclusive()
    checkMinLessThanMax()

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
      "type" -> Json.fromString("columnSumCheck"),
      "failed" -> Json.fromBoolean(failed)
    ))

    val base = ColumnSumCheck.encoder(this)
    base.deepMerge(additionalFieldsForReport)
  }
}

object ColumnSumCheck {
  val encoder: Encoder[ColumnSumCheck] = deriveEncoder[ColumnSumCheck]
  val decoder: Decoder[ColumnSumCheck] = deriveDecoder[ColumnSumCheck]
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = decoder.apply(c)
}
