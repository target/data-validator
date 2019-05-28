package com.target.data_validator.validator

import com.target.data_validator.{ValidatorError, ValidatorGood, ValidatorTimer, VarSubstitution}
import com.typesafe.scalalogging.LazyLogging
import io.circe.{DecodingFailure, HCursor, Json}
import io.circe.syntax._
import org.apache.spark.sql.{Column, DataFrame}

case class UniqueCheck(columns: Seq[String]) extends CostlyCheck {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val newColumns = columns.map(getVarSub(_, "columns", dict))
    val ret = UniqueCheck(newColumns)
    this.getEvents.foreach(ret.addEvent)
    ret
  }

  override def configCheck(df: DataFrame): Boolean = {
    columns.exists(findColumnInDataFrame(df, _).isEmpty)
  }

  override def toJson: Json = {
    import com.target.data_validator.JsonEncoders.eventEncoder
    val fields = Seq(
      ("type", Json.fromString("uniqueCheck")),
      ("columns", Json.fromValues(columns.map(Json.fromString))),
      ("failed", Json.fromBoolean(failed)),
        ("events", this.getEvents.asJson))

    Json.fromFields(fields)
  }

  override def costlyCheck(df: DataFrame): Boolean = {
    val cols = columns.map(new Column(_))
    val timer = new ValidatorTimer(s"UniqueCheck($columns)")
    addEvent(timer)
    val ret = timer.time(df.select(cols: _*).groupBy(cols: _*).count().where("count > 1").count())
    logger.info(s"costlyCheck: cols:$cols ret:$ret")
    if (ret > 0) {
      addEvent(ValidatorError(s"duplicates found $ret!"))
    } else {
      addEvent(ValidatorGood("no duplicates found."))
    }

    failed
  }
}

object UniqueCheck extends LazyLogging {

  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val columns = c.downField("columns").as[Seq[String]]
    columns.right.map(UniqueCheck(_))
  }

}
