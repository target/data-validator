package com.target.data_validator.validator

import com.target.data_validator.JsonEncoders.eventEncoder
import com.target.data_validator.VarSubstitution
import com.typesafe.scalalogging.LazyLogging
import io.circe.{DecodingFailure, HCursor, Json}
import io.circe.syntax._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Expression, IsNull}
import org.apache.spark.sql.types.StructType

case class NullCheck(column: String, threshold: Option[String]) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = NullCheck(getVarSub(column, "column", dict), threshold.map(getVarSub(_, "threshold", dict)))
    getEvents.foreach(ret.addEvent)
    ret
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression = IsNull(UnresolvedAttribute(column))

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("nullCheck")),
    ("column", Json.fromString(column)),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )
}

object NullCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val threshold = c.downField("threshold").as[String].right.toOption

    logger.debug(s"Parsing NullCheck(column:$column, threshold:$threshold) config.")
    scala.util.Right(NullCheck(column, threshold))
  }

}
