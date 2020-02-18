package com.target.data_validator.validator

import java.util.concurrent.atomic.AtomicInteger

import com.target.data_validator.{ValidatorError, VarSubstitution}
import com.target.data_validator.validator.JsonConverters._
import com.target.data_validator.validator.SumOfNumericColumnCheck.InclusiveThreshold
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.CursorOp.DownField
import io.circe.generic.semiauto._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.NumericType

case class SumOfNumericColumnCheck(
  column: String,
  thresholdType: String,
  threshold: Option[Json] = None,
  lowerBound: Option[Json] = None,
  upperBound: Option[Json] = None
)
  // XXX: I think this condTest makes this only work on integer columns???
  extends ColumnBased(column, ValidatorBase.I0) {

  override def name: String = "SumOfNumericColumn"

  lazy val thresholdOperator: Either[InclusiveThreshold.Error, InclusiveThreshold] = {
    InclusiveThreshold.fromCheck(thresholdType, threshold, lowerBound, upperBound)
  }

  private val sum = new AtomicInteger(0)

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    val rowValue = r.getAs[Int](column)
    val sumAfterRow = sum.addAndGet(rowValue)
    true
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
    if (thresholdOperator.isLeft) {
      addEvent(ValidatorError(thresholdOperator.left.get.msg))
    }

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
      "type" -> Json.fromString("sumOfNumericColumn"),
      "sum" -> Json.fromInt(sumAtSerialization),
      "failed" -> Json.fromBoolean(!thresholdOperator.right.get.isAcceptable(sumAtSerialization))
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

  trait InclusiveThreshold {
    /**
      * Test the sum inclusively
      * @param sum the value to test
      * @return true if the sum hasn't broken the threshold, false if it has
      */
    def isAcceptable(sum: Int): Boolean
    def toJsonFields: Iterable[(String, String)]
    def asJson: Json = {
      Json.fromFields(toJsonFields.map(stringPair2StringJsonPair))
    }
  }
  trait WatermarkInclusiveThreshold extends InclusiveThreshold {
    def threshold: Int

    override def toJsonFields: Iterable[(String, String)] = List(
      "thresholdType" -> this.getClass.getSimpleName,
      "threshold" -> threshold.toString
    )
  }
  trait RangedInclusiveThreshold extends InclusiveThreshold {
    def lowerBound: Int
    def upperBound: Int
    lazy val range: Range.Inclusive = lowerBound.to(upperBound)

    override def toJsonFields: Iterable[(String, String)] = List(
      "thresholdType" -> this.getClass.getSimpleName,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString
    )
  }
  case class Over(threshold: Int) extends WatermarkInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = sum <= threshold
  }
  case class Under(threshold: Int) extends WatermarkInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = sum >= threshold
  }
  case class Between(lowerBound: Int, upperBound: Int) extends RangedInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = range.contains(sum)
  }
  case class Outside(lowerBound: Int, upperBound: Int) extends RangedInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = !range.contains(sum)
  }

  object InclusiveThreshold extends LazyLogging {

    def fromCheck(thresholdType: String,
                  threshold: Option[Json],
                  lowerBound: Option[Json],
                  upperBound: Option[Json]): Either[Error, InclusiveThreshold] = {
      val operator = thresholdType match {
        case "over" =>
          optionalJsonToInt(threshold)
            .map(Over)
            .toRight(Error(s"The threshold required for $thresholdType was missing or invalid: [$threshold]"))
        case "under" =>
          optionalJsonToInt(threshold)
            .map(Under)
            .toRight(Error(s"The threshold required for $thresholdType was missing or invalid: [$threshold"))
        case "between" =>
          val op = for {
            lower <- optionalJsonToInt(lowerBound)
            upper <- optionalJsonToInt(upperBound)
          } yield Between(lower, upper)
          op.toRight(
            Error(s"""A bound required for $thresholdType was missing or invalid: [$lowerBound -> $upperBound]"""))
        case "outside" =>
          val op = for {
            lower <- optionalJsonToInt(lowerBound)
            upper <- optionalJsonToInt(upperBound)
          } yield Outside(lower, upper)
          op.toRight(
            Error(s"""A bound required for $thresholdType was missing or invalid: [$lowerBound -> $upperBound]"""))
        case _ =>
          Left(Error(s"""No threshold operator for type [$thresholdType]"""))
      }
      operator
    }

    private def optionalJsonToInt(jsonWithNumber: Option[Json]): Option[Int] = {
      jsonWithNumber.flatMap(_.asNumber.flatMap(_.toInt))
    }

    case class Error(msg: String)

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
  }
}
