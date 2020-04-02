package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorError, VarSubstitution}
import io.circe._
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

case class ColumnSumCheck(
  column: String,
  minValue: Option[Json] = None,
  maxValue: Option[Json] = None,
  inclusive: Option[Json] = None
) extends ColumnBased(column, Sum(UnresolvedAttribute(column)).toAggregateExpression()) {

  private val minOrMax: Try[Unit] = Try {
    (minValue, maxValue) match {
      case (None, None) => throw new Exception("'minValue' or 'maxValue' or both must be defined")
      case _ =>
    }
  }

  private val lowerBound: Try[Double] = Try {
    minValue match {
      case Some(json) => json.asNumber match {
        case Some(number) => number.toDouble
        case None => throw new Exception(s"'minValue' defined but type is not a Number, is: ${json.name}")
      }
      case None => Double.MinValue
    }
  }

  private val upperBound: Try[Double] = Try {
    maxValue match {
      case Some(json) => json.asNumber match {
        case Some(number) => number.toDouble
        case None => throw new Exception(s"'maxValue' defined but type is not a Number, is: ${json.name}")
      }
      case None => Double.MaxValue
    }
  }

  private val minLessThanMax: Try[Unit] = Try {
    (lowerBound, upperBound) match {
      case (Success(lower), Success(upper)) if lower >= upper =>
        throw new Exception(s"'minValue': $lower must be less than 'maxValue': $upper")
      case _ =>
    }
  }

  private val inclusiveBounds: Try[Boolean] = Try {
    inclusive match {
      case Some(json) => json.asBoolean match {
        case Some(bool) => bool
        case None => throw new Exception(s"'inclusive' defined but type is not Boolean, is: ${json.name}")
      }
      case None => false
    }
  }

  override def name: String = "columnSumCheck"

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {

    def evaluate(sum: Double): Boolean = {
      if (inclusiveBounds.get) { sum > upperBound.get || sum < lowerBound.get}
      else { sum >= upperBound.get || sum <= lowerBound.get}
    }

    failed = r.schema(idx).dataType match {
      case ShortType => evaluate(r.getShort(idx))
      case IntegerType => evaluate(r.getInt(idx))
      case LongType => evaluate(r.getLong(idx))
      case FloatType => evaluate(r.getFloat(idx))
      case DoubleType => evaluate(r.getDouble(idx))
      case ut => throw new Exception(s"Unsupported type for $name found in schema: $ut")
    }

    addEvent(ValidatorCheckEvent(failed, s"$name on '$column'", count, 1))
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
      case Failure(x) =>
        val msg = x.getMessage
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
