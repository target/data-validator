package com.target.data_validator.validator

import com.target.data_validator._
import com.target.data_validator.stats._
import io.circe
import io.circe.Json
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.types.NumericType

case class ColStats(column: String)
  extends ColumnBased(column, (new FirstPassStatsAggregator)(new Column(UnresolvedAttribute(column))).expr)
{
  import ValidatorBase._
  import com.target.data_validator.JsonEncoders.eventEncoder

  override def name: String = "colstats"

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
    val rStats = r.getStruct(idx)
    val count = rStats.getLong(0)
    val mean = rStats.getDouble(1)
    val min = rStats.getDouble(2)
    val max = rStats.getDouble(3)
    val json = Json.obj(
        ("name", Json.fromString("FirstPassStats")),
        ("column", Json.fromString(column)),
        ("count", Json.fromLong(count)),
        ("min", Json.fromDoubleOrNull(min)),
        ("mean", Json.fromDoubleOrNull(mean)),
        ("max", Json.fromDoubleOrNull(max)))

    logger.info(s"VarJsonEvent:${json.spaces2}")

    // TODO: integration between ColStats and reporting JSON
    addEvent(JsonEvent(json))

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
}

object ColStats {
  implicit val encoder: circe.Encoder[ColumnSumCheck] = deriveEncoder[ColumnSumCheck]
  implicit val decoder: circe.Decoder[ColumnSumCheck] = deriveDecoder[ColumnSumCheck]
}
