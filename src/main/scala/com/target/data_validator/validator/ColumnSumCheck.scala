package com.target.data_validator.validator

import com.target.data_validator.{ColumnBasedValidatorCheckEvent, JsonEncoders, ValidatorError, VarSubstitution}
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

import scala.collection.immutable.ListMap

case class ColumnSumCheck(
    column: String,
    minValue: Option[Json] = None,
    maxValue: Option[Json] = None,
    inclusive: Option[Json] = None
) extends ColumnBased(column, Sum(UnresolvedAttribute.quoted(column)).toAggregateExpression()) {

  private val minOrMax: Either[String, Unit] = if (minValue.isEmpty && maxValue.isEmpty) {
    Left("'minValue' or 'maxValue' or both must be defined")
  } else {
    Right()
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

    val dataType = r.schema(idx).dataType
    val isInclusive = inclusiveBounds.right.get
    val lowerBoundValue = lowerBound.right.get
    val upperBoundValue = upperBound.right.get

    def evaluate(sum: Double): Boolean = {
      if (isInclusive) { sum > upperBoundValue || sum < lowerBoundValue }
      else { sum >= upperBoundValue || sum <= lowerBoundValue }
    }

    def getPctError(sum: Double): String = {
      if (sum < lowerBoundValue) {
        calculatePctError(lowerBoundValue, sum)
      } else if (sum > upperBoundValue) {
        calculatePctError(upperBoundValue, sum)
      } else if (!isInclusive && (sum == upperBoundValue || sum == lowerBoundValue)) {
        "undefined"
      } else {
        "0.00%"
      }
    }

    def getData(pctError: String): ListMap[String, String] = {
      val initial: ListMap[String, String] = ((minValue, maxValue) match {
        case (Some(x), Some(y)) =>
          ListMap("lower_bound" -> x.asNumber.get.toString, "upper_bound" -> y.asNumber.get.toString)
        case (None, Some(y)) => ListMap("upper_bound" -> y.asNumber.get.toString)
        case (Some(x), None) => ListMap("lower_bound" -> x.asNumber.get.toString)
        case (None, None) => throw new RuntimeException("Must define at least one of minValue or maxValue.")
      })
      initial ++ List("inclusive" -> isInclusive.toString, "actual" -> r(idx).toString, "relative_error" -> pctError)
    }

    val actualSum: Double = dataType match {
      case ByteType => r.getByte(idx)
      case ShortType => r.getShort(idx)
      case IntegerType => r.getInt(idx)
      case LongType => r.getLong(idx)
      case FloatType => r.getFloat(idx)
      case DoubleType => r.getDouble(idx)
      case ut => throw new Exception(s"Unsupported type for $name found in schema: $ut")
    }

    failed = evaluate(actualSum)
    val pctError = getPctError(actualSum)
    val data = getData(pctError)

    val bounds = minValue.getOrElse(" ") :: maxValue.getOrElse("") :: Nil
    val prettyBounds = if (isInclusive) {
      bounds.mkString("[", ", ", "]")
    } else {
      bounds.mkString("(", ", ", ")")
    }

    val msg =
      s"$name on $column[$dataType]: Expected Range: $prettyBounds Actual: ${r(idx)} Relative Error: $pctError"
    addEvent(ColumnBasedValidatorCheckEvent(failed, data, msg))
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
    import JsonEncoders.eventEncoder
    val additionalFieldsForReport = Json.fromFields(
      Set(
        "type" -> Json.fromString("columnSumCheck"),
        "failed" -> Json.fromBoolean(failed),
        "events" -> getEvents.asJson
      )
    )

    val base = ColumnSumCheck.encoder(this)
    base.deepMerge(additionalFieldsForReport)
  }
}

object ColumnSumCheck {
  val encoder: Encoder[ColumnSumCheck] = deriveEncoder[ColumnSumCheck]
  val decoder: Decoder[ColumnSumCheck] = deriveDecoder[ColumnSumCheck]
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = decoder.apply(c)
}
