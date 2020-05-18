package com.target.data_validator.validator

import com.target.data_validator._
import com.target.data_validator.stats._
import io.circe
import io.circe.Json
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.NumericType

import scala.concurrent.Promise
import scala.util._

/**
  * This validator implements both [[CheapCheck]] via [[ColumnBased]] and [[CostlyCheck]].
  *
  * A second pass aggregation performed in the costlyCheck call produces histograms
  *  describing the distribution of numeric values.
  *
  * @param column the column to collect stats on
  */
case class ColStats(column: String)
  extends ColumnBased(column, (new FirstPassStatsAggregator)(new Column(UnresolvedAttribute(column))).expr)
   with CostlyCheck
{
  import ValidatorBase._
  import com.target.data_validator.JsonEncoders.eventEncoder

  override def name: String = "colstats"

  private val promiseToDoFirstPass: Promise[FirstPassStats] = Promise()

  override def configCheck(df: DataFrame): Boolean = {
    if (isColumnInDataFrame(df, column)) {
      df.schema(column).dataType match {
        case _: NumericType => false
        case badType =>
          val msg = s"Column $name type:$badType is not Numeric."
          logger.error(msg)
          addEvent(ValidatorError(msg))
          failed
      }
    } else {
      val msg = s"Column $name not in data frame."
      logger.error(msg)
      addEvent(ValidatorError(msg))
      failed
    }
  }

  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    promiseToDoFirstPass complete Try {
      val rStats = r.getStruct(idx)

      FirstPassStats(
        count = rStats.getLong(0),
        mean = rStats.getDouble(1),
        min = rStats.getDouble(2),
        max = rStats.getDouble(3)
      )
    }

    false
  }

  override def substituteVariables(
    dict: VarSubstitution
  ): ValidatorBase = this

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("colstats")),
    ("column", Json.fromString(column)),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )

  override def costlyCheck(df: DataFrame): Boolean = {
    import df.sparkSession.implicits._

    promiseToDoFirstPass.future.value match {
      case None =>
        // This shouldn't be possible for users as the validator job executes quick checks AND THEN costly checks.
        logger.error(
          "ColStats costly histograms requires that 'quick checks' " +
          "are executed first to generate first pass stats.")

        true
      case Some(Success(firstPassStats)) =>
        val agg = new SecondPassStatsAggregator(firstPassStats)
        val secondPassStats = df.select(agg(col(column))).as[SecondPassStats].head
        val completeStats = CompleteStats(name = s"`$column` stats", column = column, firstPassStats, secondPassStats)
        val json = completeStats.asJson

        logger.info(s"VarJsonEvent:${json.spaces2}")
        addEvent(JsonEvent(json))

        false
      case Some(Failure(e)) =>
        logger.error("Error producting first pass of colstats", e)

        true
    }
  }

}

object ColStats {
  implicit val encoder: circe.Encoder[ColumnSumCheck] = deriveEncoder[ColumnSumCheck]
  implicit val decoder: circe.Decoder[ColumnSumCheck] = deriveDecoder[ColumnSumCheck]
}
