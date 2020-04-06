package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorError, VarSubstitution}
import io.circe._
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

case class ColumnSumCheck(
  column: String,
  minValue: Option[Json] = None,
  maxValue: Option[Json] = None,
  inclusive: Option[Json] = None
) extends ColumnBased(column, Sum(UnresolvedAttribute(column)).toAggregateExpression()) {

  private val minOrMax: Either[String, Unit] = if (minValue.isEmpty && maxValue.isEmpty) {
      Left("'minValue' or 'maxValue' or both must be defined")
    } else {
      Right()
    }
  }

  private val lowerBound: Either[String, Double] = minValue match {
    case Some(json) =>
      if (json.isNumber) { Right(json.asNumber.get.toDouble) }
      else { Left(s"'minValue' defined but type is not a Number, is: ${json.name}") }
    case None => Right(Double.MinValue)
  }

  private val upperBound: Either[String, Double] = maxValue match {
    case Some(json) =>
      if (json.isNumber) { Right(json.asNumber.get.toDouble) }
      else { Left(s"'maxValue' defined but type is not a Number, is: ${json.name}") }
    case None => Right(Double.MaxValue)
  }

  private val minLessThanMax: Either[String, Unit] = (lowerBound, upperBound) match {
    case (Right(lower), Right(upper)) if lower >= upper =>
      Left(s"'minValue': $lower must be less than 'maxValue': $upper")
    case _ => Right()
  }

  private val inclusiveBounds: Either[String, Boolean] = inclusive match {
    case Some(json) =>
      if (json.isBoolean) { Right(json.asBoolean.get) }
      else { Left(s"'inclusive' defined but type is not Boolean, is: ${json.name}") }
    case None => Right(false)
  }

  override def name: String = "columnSumCheck"

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {

    def evaluate(sum: Double): Boolean = {
      if (inclusiveBounds.right.get) { sum > upperBound.right.get || sum < lowerBound.right.get}
      else { sum >= upperBound.right.get || sum <= lowerBound.right.get}
    }

    failed = r.schema(idx).dataType match {
      case ShortType => evaluate(r.getShort(idx))
      case IntegerType => evaluate(r.getInt(idx))
      case LongType => evaluate(r.getLong(idx))
      case FloatType => evaluate(r.getFloat(idx))
      case DoubleType => evaluate(r.getDouble(idx))
      case ut => throw new Exception(s"Unsupported type for $name found in schema: $ut")
    }

    val bounds = minValue.getOrElse("-Inf") :: maxValue.getOrElse("Inf") :: Nil
    val prettyBounds = if (inclusiveBounds.right.get) {
      r.get(idx) + " in " + bounds.mkString("[", " , ", "]")
    } else {
      r.get(idx) + " in " + bounds.mkString("(", " , ", ")")
    }
    val errorValue = if (failed) 1 else 0
    addEvent(ValidatorCheckEvent(failed, s"$name on '$column': $prettyBounds", count, errorValue))
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
    Seq(
      minOrMax,
      lowerBound,
      upperBound,
      minLessThanMax,
      inclusiveBounds
    ).foreach {
      case Left(msg) =>
        logger.error(msg)
        addEvent(ValidatorError(msg))
      case _ =>
    }

    findColumnInDataFrame(df, column) match {
      case Some(ft) if ft.dataType.isInstanceOf[NumericType] =>
      case Some(ft) =>
        val msg = s"Column: $column found, but not of numericType type: ${ft.dataType}"
        logger.error(msg)
        addEvent(ValidatorError(msg))
      case None =>
        val msg = s"Column: $column not found in schema"
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
