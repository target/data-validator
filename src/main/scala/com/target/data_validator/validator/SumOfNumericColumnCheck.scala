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

case class SumOfNumericColumnCheck(column: String, threshold: InclusiveThreshold)
  // XXX: I think this condTest makes this only work on integer columns???
  extends ColumnBased(column, ValidatorBase.I0) {

  override def name: String = "SumOfNumericColumn"

  private val sum = new AtomicInteger(0)

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    val rowValue = r.getAs[Int](column)
    val sumAfterRow = sum.addAndGet(rowValue)
    true
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = copy(
      column = getVarSub(column, "column", dict),
      // FIXME: wtf lol
      threshold = getVarSubJson(threshold.asJson, "value", dict)
        .as[InclusiveThreshold](InclusiveThreshold.Decoder)
        .right
        .get
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
    val sumAtSerialization = sum.get()
    Json.obj(
      ("type", Json.fromString("sumOfNumericColumn")),
      ("column", Json.fromString(column)),
      ("sum", Json.fromInt(sumAtSerialization)),
      ("threshold", threshold.asJson),
      ("failed", Json.fromBoolean(threshold.isAcceptable(sumAtSerialization)))
    )
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
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    for {
      column <- c.downField("column").as[String].right
      threshold <- c.downField("threshold").as[InclusiveThreshold](InclusiveThreshold.Decoder).right
    } yield {
      logger.debug(s"Parsing SumOfNumericColumnCheck(column:$column, threshold:$threshold) config.")
      SumOfNumericColumnCheck(column, threshold)
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
      "type" -> this.getClass.getSimpleName,
      "threshold" -> threshold.toString
    )
  }
  trait RangedInclusiveThreshold extends InclusiveThreshold {
    def lower: Int
    def upper: Int
    lazy val range: Range.Inclusive = lower.to(upper)

    override def toJsonFields: Iterable[(String, String)] = List(
      "type" -> this.getClass.getSimpleName,
      "lower" -> lower.toString,
      "upper" -> upper.toString
    )
  }
  case class Over(threshold: Int) extends WatermarkInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = sum <= threshold
  }
  case class Under(threshold: Int) extends WatermarkInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = sum >= threshold
  }
  case class Between(lower: Int, upper: Int) extends RangedInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = range.contains(sum)
  }
  case class Outside(lower: Int, upper: Int) extends RangedInclusiveThreshold {
    override def isAcceptable(sum: Int): Boolean = !range.contains(sum)
  }

  object InclusiveThreshold extends LazyLogging {
    object Decoder extends Decoder[InclusiveThreshold] {
      val inclusiveThresholdTypes: Map[String, Decoder[InclusiveThreshold]] = { Map(
          "over" -> deriveDecoder[Over].asInstanceOf[Decoder[InclusiveThreshold]],
          "under" -> deriveDecoder[Under].asInstanceOf[Decoder[InclusiveThreshold]],
          "between" -> deriveDecoder[Between].asInstanceOf[Decoder[InclusiveThreshold]],
          "outside" -> deriveDecoder[Outside].asInstanceOf[Decoder[InclusiveThreshold]]
        )
      }

      def apply(c: HCursor): Either[DecodingFailure, InclusiveThreshold] = {
        c.downField("type").as[String]
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
