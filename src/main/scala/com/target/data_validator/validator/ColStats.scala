package com.target.data_validator.validator

import com.target.data_validator._
import com.target.data_validator.stats._
import io.circe
import io.circe.Json
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StructType}

import scala.concurrent.Promise
import scala.util._

/** This validator implements a set of column metrics on a specified column by performing two I/O passes over the
  * input table.
  *
  * @param column
  *   the column to collect stats on
  */
final case class ColStats(column: String) extends TwoPassCheapCheck {
  import ValidatorBase._
  import com.target.data_validator.JsonEncoders.eventEncoder

  private val tempColumnName = column + "_" + hashCode

  override def name: String = "colstats"

  private val promiseToDoFirstPass: Promise[FirstPassStats] = Promise()

  // invoke only after first pass has completed
  private def unsafeFirstPassAccessor = promiseToDoFirstPass.future.value match {
    case None =>
      throw new IllegalStateException(
        "ColStats costly histograms requires that the pre-processing projections " +
          "are executed first to generate first pass stats."
      )
    case Some(Success(firstPassStats)) =>
      firstPassStats
    case Some(Failure(e)) =>
      throw e
  }

  // expression to aggregate first pass of stats
  override def firstPassSelect(): Column = {
    val firstPassAgg = new FirstPassStatsAggregator
    firstPassAgg(new Column(UnresolvedAttribute(column))) as tempColumnName
  }

  // extract first pass stats from output row
  override def sinkFirstPassRow(row: Row): Unit = {
    promiseToDoFirstPass complete Try {
      val rStats = row.getAs[Row](tempColumnName)
      FirstPassStats.fromRowRepr(rStats)
    }
  }

  // generate second pass ("Quick Check") expression
  // NOTE: this call implicitly REQUIRES that the first pass has completed
  override def select(schema: StructType, dict: VarSubstitution): Expression = {
    val agg = new SecondPassStatsAggregator(unsafeFirstPassAccessor)
    agg(col(column)).expr
  }

  // construct complete stats from quick check output row
  override def quickCheck(r: Row, count: Long, idx: Int): Boolean = {
    val rStats = r.getAs[Row](idx)
    val secondPassStats = SecondPassStats.fromRowRepr(rStats)
    val completeStats = CompleteStats(
      name = s"`$column` stats",
      column = column,
      firstPassStats = unsafeFirstPassAccessor,
      secondPassStats = secondPassStats
    )

    val json = completeStats.asJson
    logger.info(s"VarJsonEvent:${json.spaces2}")
    addEvent(JsonEvent(json))

    false
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = this

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

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("colstats")),
    ("column", Json.fromString(column)),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )

}

object ColStats {
  implicit val encoder: circe.Encoder[ColumnSumCheck] = deriveEncoder[ColumnSumCheck]
  implicit val decoder: circe.Decoder[ColumnSumCheck] = deriveDecoder[ColumnSumCheck]
}
