package com.target.data_validator.validator

import com.target.data_validator.{JsonEncoders, ValidatorError, VarSubstitution}
import com.target.data_validator.validator.ValidatorBase._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{DecodingFailure, HCursor, Json}
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, StructType}

case class StringRegexCheck(
  column: String,
  regex: Option[Json],
  threshold: Option[String]
) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {

    val ret = StringRegexCheck(
      getVarSub(column, "column", dict),
      regex.map(getVarSubJson(_, "regex", dict)),
      threshold.map(getVarSub(_, "threshold", dict))
    )
    getEvents.foreach(ret.addEvent)
    ret
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression = {

    val colExp = UnresolvedAttribute(column)

    val regexExpression = regex.map { r => RLike(colExp, createLiteralOrUnresolvedAttribute(StringType, r)) }

    val ret = regexExpression match {
      /*
        RLike returns false if the column value is null.
        To avoid counting null values as validation failures (like other validations),
        an explicit non null check on the column value is required.
       */
      case Some(x) => And(Not(x), IsNotNull(colExp))
      case _ => throw new RuntimeException("Must define a regex.")
    }
    logger.debug(s"Expr: $ret")
    ret
  }

  override def configCheck(df: DataFrame): Boolean = {

    // Verify if regex is specified.
    val values = (regex::Nil).flatten
    if (values.isEmpty) {
      addEvent(ValidatorError("Must define a regex."))
    }

    // Verify that the data type of the specified column is a String.
    val colType = findColumnInDataFrame(df, column)
    if (colType.isDefined) {
      val dataType = colType.get.dataType
      if (!dataType.isInstanceOf[StringType]) {
        addEvent(ValidatorError(s"Data type of column '$column' must be String, but was found to be $dataType"))
      }
    }

    failed
  }

  override def toJson: Json = {
    import JsonEncoders.eventEncoder
    val fields = Seq(
      ("type", Json.fromString("stringRegexCheck")),
      ("column", Json.fromString(column))
    ) ++
      regex.map(r => ("regex", r)) ++
      Seq(
        ("events", getEvents.asJson)
      )
    Json.obj(fields: _*)
  }
}

object StringRegexCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val regex = c.downField("regex").as[Json].right.toOption
    val threshold = c.downField("threshold").as[String].right.toOption

    logger.debug(s"column: $column")
    logger.debug(s"regex: $regex type: ${regex.getClass.getCanonicalName}")
    logger.debug(s"threshold: $threshold type: ${threshold.getClass.getCanonicalName}")

    c.focus.foreach {f => logger.info(s"StringRegexCheckJson: ${f.spaces2}")}
    scala.util.Right(StringRegexCheck(column, regex, threshold))
  }
}
