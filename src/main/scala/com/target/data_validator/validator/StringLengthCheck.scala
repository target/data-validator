package com.target.data_validator.validator

import com.target.data_validator.JsonUtils.debugJson
import com.target.data_validator.validator.ValidatorBase._
import com.target.data_validator.{JsonEncoders, ValidatorError, VarSubstitution}
import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import io.circe.{DecodingFailure, HCursor, Json}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

case class StringLengthCheck(
  column: String,
  minLength: Option[Json],
  maxLength: Option[Json],
  threshold: Option[String]
) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {

    val ret = StringLengthCheck(
      getVarSub(column, "column", dict),
      minLength.map(getVarSubJson(_, "minLength", dict)),
      maxLength.map(getVarSubJson(_, "maxLength", dict)),
      threshold.map(getVarSub(_, "threshold", dict))
    )
    getEvents.foreach(ret.addEvent)
    ret
  }

  private def cmpExpr(colExpr: Expression,
    value: Option[Json],
    cmp: (Expression, Expression) => Expression
  ): Option[Expression] = {
    value.map { v => cmp(colExpr, createLiteralOrUnresolvedAttribute(IntegerType, v)) }
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression = {

    val colExp = Length(UnresolvedAttribute(column))

    val minLengthExpression = cmpExpr(colExp, minLength, LessThan)
    val maxLengthExpression = cmpExpr(colExp, maxLength, GreaterThan)

    val ret = (minLengthExpression, maxLengthExpression) match {
      case (Some(x), None) => x
      case (None, Some(y)) => y
      case (Some(x), Some(y)) => Or(x, y)
      case _ => throw new RuntimeException("Must define min or max value.")
    }
    logger.debug(s"Expr: $ret")
    ret
  }

  private def checkMinLessThanOrEqualToMax(values: List[Json]): Unit = {

    if (values.forall(_.isNumber)) {
      values.flatMap(_.asNumber) match {
        case mv :: xv :: Nil if mv.toDouble > xv.toDouble =>
          addEvent(ValidatorError(s"min: ${minLength.get} must be less than or equal to max: ${maxLength.get}"))
        case _ =>
      }
    } else if (values.forall(_.isString)) {
      values.flatMap(_.asString) match {
        case mv :: xv :: Nil if mv == xv =>
          addEvent(ValidatorError(s"Min[String]: $mv must be less than max[String]: $xv"))
        case _ =>
      }
    } else {
      // Not Strings or Numbers
      addEvent(ValidatorError(s"Unsupported type in ${values.map(debugJson).mkString(", ")}"))
    }
  }

  override def configCheck(df: DataFrame): Boolean = {

    // Verify if at least one of min or max is specified.
    val values = (minLength::maxLength::Nil).flatten
    if (values.isEmpty) {
      addEvent(ValidatorError("Must define minLength or maxLength or both."))
    }

    // Verify that min is less than max
    checkMinLessThanOrEqualToMax(values)

    // Verify that the data type of the specified column is a String.
    val colType = findColumnInDataFrame(df, column)
    if (colType.isDefined) {
      val dataType = colType.get.dataType
      if (!(dataType.isInstanceOf[StringType])) {
        addEvent(ValidatorError(s"Data type of column '$column' must be String, but was found to be $dataType"))
      }
    }

    failed
  }

  override def toJson: Json = {
    import JsonEncoders.eventEncoder
    val fields = Seq(
      ("type", Json.fromString("stringLengthCheck")),
      ("column", Json.fromString(column))
    ) ++
      minLength.map(mv => ("minLength", mv)) ++
      maxLength.map(mv => ("maxLength", mv)) ++
      Seq(
        ("events", getEvents.asJson)
      )
    Json.obj(fields: _*)
  }
}

object StringLengthCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val minLengthJ = c.downField("minLength").as[Json].right.toOption
    val maxLengthJ = c.downField("maxLength").as[Json].right.toOption
    val threshold = c.downField("threshold").as[String].right.toOption

    logger.debug(s"column: $column")
    logger.debug(s"minLength: $minLengthJ type: ${minLengthJ.getClass.getCanonicalName}")
    logger.debug(s"maxLength: $maxLengthJ type: ${maxLengthJ.getClass.getCanonicalName}")
    logger.debug(s"threshold: $threshold type: ${threshold.getClass.getCanonicalName}")

    c.focus.foreach {f => logger.info(s"StringLengthCheckJson: ${f.spaces2}")}
    scala.util.Right(StringLengthCheck(column, minLengthJ, maxLengthJ, threshold))
  }
}
