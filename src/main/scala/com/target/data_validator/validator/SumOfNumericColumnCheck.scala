package com.target.data_validator.validator

import java.util.concurrent.atomic.AtomicInteger

import com.target.data_validator.{ValidatorCheckEvent, ValidatorError, VarSubstitution}
import com.target.data_validator.validator.JsonConverters._
import com.target.data_validator.validator.SumOfNumericColumnCheck.InclusiveThreshold
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.CursorOp.DownField
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThan, If, LessThan, Literal, Not, Or}
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.types._

import scala.math.Numeric.Implicits._
import scala.math.Ordering.Implicits._

case class SumOfNumericColumnCheck(
  column: String,
  thresholdType: String,
  threshold: Option[Json] = None,
  lowerBound: Option[Json] = None,
  upperBound: Option[Json] = None
)
  extends ColumnBased(column, Sum(UnresolvedAttribute(column)).toAggregateExpression()) {

  override def name: String = "SumOfNumericColumn"

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    val dataType = r.schema(idx).dataType
    val rawValueForLogging = r.get(idx)

    val rowValueAsExpr: Expression = Literal.create(rawValueForLogging, dataType)
    // by the time this is executed, the options have been verified
    lazy val thresholdAsExpr: Expression = ValidatorBase.createLiteral(dataType, threshold.get)
    lazy val lowerBoundAsExpr: Expression = ValidatorBase.createLiteral(dataType, lowerBound.get)
    lazy val upperBoundAsExpr: Expression = ValidatorBase.createLiteral(dataType, upperBound.get)

    val failedIfTrueExpr = thresholdType match {
      case "over" if threshold.isDefined =>
        Not(GreaterThan(rowValueAsExpr, thresholdAsExpr))
      case "under" if threshold.isDefined =>
        Not(LessThan(rowValueAsExpr, thresholdAsExpr))
      case "between" if lowerBound.isDefined && upperBound.isDefined =>
        Not(And(GreaterThan(rowValueAsExpr, lowerBoundAsExpr), LessThan(rowValueAsExpr, upperBoundAsExpr)))
      case "outside" if lowerBound.isDefined && upperBound.isDefined =>
        Not(Or(LessThan(rowValueAsExpr, lowerBoundAsExpr), GreaterThan(rowValueAsExpr, upperBoundAsExpr)))
      case _ =>
        val msg = s"""
                     |Unknown threshold type $thresholdType or one of the follow is required and not present:
                     |threshold: $threshold, lowerBound: $lowerBound, upperBound: $upperBound
              """.stripMargin
        logger.error(msg)
        addEvent(ValidatorError(msg))
        Literal.TrueLiteral
    }

    val eval = failedIfTrueExpr.eval()
    eval.asInstanceOf[Boolean]
  }

  import InclusiveThreshold.Implicits._
  def thresholdOperator[T: Numeric](dataType: DataType): Either[InclusiveThreshold.Error, InclusiveThreshold[T]] = {
    /*
    implicit val converter: Option[Json] => Option[T] = dataType match {
      case IntegerType => optionalJsonToInt _
      case LongType => optionalJsonToLong _
    }
    */

    import Decoder._
    implicit def numericDecoder: Decoder[T] = new Decoder[T] {
      override def apply(c: HCursor): Result[T] = {
        // try all of the types until one works?
        /*
        dataType match {
          case IntegerType => Decoder.decodeInt(c).right.map(_.asInstanceOf[Result[T]])
        }
        */
        ???
      }
    }
    implicit val converter: Option[Json] => Option[T] = {
      _ flatMap { json => implicitly[Decoder[T]].decodeJson(json).right.toOption }
    }

    InclusiveThreshold.fromCheck[T](thresholdType, threshold, lowerBound, upperBound)
  }

  private val sum = new AtomicInteger(0)

  def quickCheckOLD(r: Row, count: Long, idx: Int): Boolean = {
    val dataType = r.schema(idx).dataType
    val rawValueForLogging = r.get(idx)

    // val operator: InclusiveThreshold[_] = thresholdOperator[Int].right.get

    // logger.debug(s"${operator} evaluating [$rawValueForLogging]")

    failed = dataType match {
      case IntegerType =>
        thresholdOperator[Int](IntegerType).right.get.isUnacceptable(r.getInt(idx))
      case ShortType =>
        thresholdOperator[Short](ShortType).right.get.isUnacceptable(r.getShort(idx))
      /*
      case LongType => thresholdOperator.right.get.isUnacceptable(r.getLong(idx))
      case FloatType => thresholdOperator.right.get.isUnacceptable(r.getFloat(idx))
      case DoubleType => thresholdOperator.right.get.isUnacceptable(r.getDouble(idx))
      */
      // TODO: apparently DecimalType is not a DataType
      // case DecimalType => thresholdOperator.right.get.isUnacceptable(r.getDecimal(idx))
      case ut =>
        logger.error(s"quickCheck for type: $ut, Row: $r not Implemented! Please file this as a bug.")
        true // Fail check!
    }
    if (failed) {
      addEvent(
        ValidatorCheckEvent(
          failed,
          s"sumOfNumericColumnCheck column[$dataType]: $column value: $rawValueForLogging " +
            "doesn't satisfy ${thresholdOperator.right.get}",
          count,
          errorCount = 1
        )
      )
    }
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
    /*
    if (thresholdOperator.isLeft) {
      addEvent(ValidatorError(thresholdOperator.left.get.msg))
    }
    */

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
    val sumAtSerialization = sum.get()
    val additionalFieldsForReport = Json.fromFields(Set(
      "type" -> Json.fromString("sumOfNumericColumn")
      // "sum" -> Json.fromInt(sumAtSerialization),
      // "failed" -> Json.fromBoolean(!thresholdOperator.right.get.isAcceptable(sumAtSerialization))
    ))

    val base = SumOfNumericColumnCheck.encoder(this)
    base.deepMerge(additionalFieldsForReport)
  }
}

object JsonConverters {
  def stringPair2StringJsonPair(pair: (String, String)): (String, Json) = {
    (pair._1, Json.fromString(pair._2))
  }
}

// XXX: can we limit this to a RangedProxy?
// TODO: genericize this
object SumOfNumericColumnCheck extends LazyLogging {

  val encoder: Encoder[SumOfNumericColumnCheck] = deriveEncoder[SumOfNumericColumnCheck]

  // XXX: this looks like it could be derived
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    for {
      column <- c.downField("column").as[String].right
      thresholdType <- c.downField("thresholdType").as[String].right
      threshold <- c.downField("threshold").as[Option[Json]].right
      lowerBound <- c.downField("lowerBound").as[Option[Json]].right
      upperBound <- c.downField("upperBound").as[Option[Json]].right
    } yield {
      logger.debug(s"Parsing SumOfNumericColumnCheck(column:$column, threshold:$thresholdType) config.")
      SumOfNumericColumnCheck(column, thresholdType, threshold, lowerBound, upperBound)
    }
  }

  abstract class InclusiveThreshold[T: Numeric] {
    /**
      * Test the sum inclusively
      * @param sum the value to test
      * @return true if the sum hasn't broken the threshold, false if it has
      */
    def isAcceptable(sum: T): Boolean
    def isUnacceptable(sum: T): Boolean = !isAcceptable(sum)
    def toJsonFields: Iterable[(String, String)]
    def asJson: Json = {
      Json.fromFields(toJsonFields.map(stringPair2StringJsonPair))
    }
  }
  abstract class WatermarkInclusiveThreshold[T: Numeric] extends InclusiveThreshold[T] {
    def threshold: T

    override def toJsonFields: Iterable[(String, String)] = List(
      "thresholdType" -> this.getClass.getSimpleName,
      "threshold" -> threshold.toString
    )
  }
  abstract class RangedInclusiveThreshold[T: Numeric] extends InclusiveThreshold[T] {
    def lowerBound: T
    def upperBound: T

    override def toJsonFields: Iterable[(String, String)] = List(
      "thresholdType" -> this.getClass.getSimpleName,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString
    )
  }
  case class Over[T: Numeric](threshold: T) extends WatermarkInclusiveThreshold[T] {
    override def isAcceptable(sum: T): Boolean = sum <= threshold
  }
  case class Under[T: Numeric](threshold: T) extends WatermarkInclusiveThreshold[T] {
    override def isAcceptable(sum: T): Boolean = sum >= threshold
  }
  case class Between[T: Numeric](lowerBound: T, upperBound: T) extends RangedInclusiveThreshold[T] {
    override def isAcceptable(sum: T): Boolean = sum >= lowerBound && sum <= upperBound
  }
  case class Outside[T: Numeric](lowerBound: T, upperBound: T) extends RangedInclusiveThreshold[T] {
    override def isAcceptable(sum: T): Boolean = sum <= lowerBound && sum >= upperBound
  }

  object InclusiveThreshold extends LazyLogging {

    def fromCheck[T: Numeric](thresholdType: String,
                  threshold: Option[Json],
                  lowerBound: Option[Json],
                  upperBound: Option[Json])
                  (implicit convert: Option[Json] => Option[T]): Either[Error, InclusiveThreshold[T]] = {
      val operator = thresholdType match {
        case "over" =>
          convert(threshold)
            .map(Over[T])
            .toRight(Error(s"The threshold required for $thresholdType was missing or invalid: [$threshold]"))
        case "under" =>
          convert(threshold)
            .map(Under[T])
            .toRight(Error(s"The threshold required for $thresholdType was missing or invalid: [$threshold"))
        case "between" =>
          val op = for {
            lower <- convert(lowerBound)
            upper <- convert(upperBound)
          } yield Between[T](lower, upper)
          op.toRight(
            Error(s"""A bound required for $thresholdType was missing or invalid: [$lowerBound -> $upperBound]"""))
        case "outside" =>
          val op = for {
            lower <- convert(lowerBound)
            upper <- convert(upperBound)
          } yield Outside[T](lower, upper)
          op.toRight(
            Error(s"""A bound required for $thresholdType was missing or invalid: [$lowerBound -> $upperBound]"""))
        case _ =>
          Left(Error(s"""No threshold operator for type [$thresholdType]"""))
      }
      operator
    }

    object Implicits {
      /*
      implicit def optionalJsonToT[T: Numeric](jsonWithNumber: Option[Json]): Option[T] = {
        unboxAndConvert(jsonWithNumber)
      }
      */

      implicit def optionalJsonToInt(jsonWithNumber: Option[Json]): Option[Int] = {
        unboxAndConvert(jsonWithNumber)(_.toInt)
      }
      implicit def optionalJsonToShort(jsonWithNumber: Option[Json]): Option[Short] = {
        unboxAndConvert(jsonWithNumber)(_.toShort)
      }
      implicit def optionalJsonToLong(jsonWithNumber: Option[Json]): Option[Long] = {
        unboxAndConvert(jsonWithNumber)(_.toLong)
      }
      private def unboxAndConvert[T: Numeric](json: Option[Json])(converter: JsonNumber => Option[T]) = {
        json.flatMap(_.asNumber.flatMap(converter))
      }
    }

    case class Error(msg: String)

    /*
    object Decoder extends Decoder[InclusiveThreshold] {
      val inclusiveThresholdTypes: Map[String, Decoder[InclusiveThreshold]] = { Map(
          "over" -> deriveDecoder[Over].asInstanceOf[Decoder[InclusiveThreshold]],
          "under" -> deriveDecoder[Under].asInstanceOf[Decoder[InclusiveThreshold]],
          "between" -> deriveDecoder[Between].asInstanceOf[Decoder[InclusiveThreshold]],
          "outside" -> deriveDecoder[Outside].asInstanceOf[Decoder[InclusiveThreshold]]
        )
      }

      def apply(c: HCursor): Either[DecodingFailure, InclusiveThreshold] = {
        c.field("thresholdType").as[String]
          .right
          .map(inclusiveThresholdTypes.get)
          .right
          .map { decoder =>
            decoder.map(_.apply(c))
          }
        match {
          case Right(maybeThreshold) =>
            maybeThreshold.getOrElse(Left(DecodingFailure("no threshold for the type", List(DownField("type")))))
          case Left(decodingFailure) => throw new RuntimeException(s"Major failure: $decodingFailure")
        }
      }
    }
    */
  }
}
