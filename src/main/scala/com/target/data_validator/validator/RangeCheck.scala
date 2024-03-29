package com.target.data_validator.validator

import com.target.data_validator.{JsonEncoders, ValidatorError, VarSubstitution}
import com.target.data_validator.JsonUtils.debugJson
import com.target.data_validator.validator.ValidatorBase._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{DecodingFailure, HCursor, Json}
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, StructType}

case class RangeCheck(
    column: String,
    minValue: Option[Json],
    maxValue: Option[Json],
    inclusive: Option[Json],
    threshold: Option[String]
) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = RangeCheck(
      getVarSub(column, "column", dict),
      minValue.map(getVarSubJson(_, "minValue", dict)),
      maxValue.map(getVarSubJson(_, "maxValue", dict)),
      inclusive.map(getVarSubJson(_, "inclusive", dict)),
      threshold.map(getVarSub(_, "threshold", dict))
    )
    getEvents.foreach(ret.addEvent)
    ret
  }

  private def cmpExpr(
      colExpr: Expression,
      value: Option[Json],
      colType: DataType,
      cmp: (Expression, Expression) => Expression
  ): Option[Expression] = {
    value.map { v => cmp(colExpr, createLiteralOrUnresolvedAttribute(colType, v)) }
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression = {
    val colType = schema(column).dataType
    val colExp = UnresolvedAttribute(column)
    val (minCmpExp, maxCmpExp) = if (inclusive.flatMap(_.asBoolean).getOrElse(false)) {
      (LessThan, GreaterThan)
    } else {
      (LessThanOrEqual, GreaterThanOrEqual)
    }

    val minValueExpression = cmpExpr(colExp, minValue, colType, minCmpExp)
    val maxValueExpression = cmpExpr(colExp, maxValue, colType, maxCmpExp)

    val ret = (minValueExpression, maxValueExpression) match {
      case (Some(x), None) => x
      case (None, Some(y)) => y
      case (Some(x), Some(y)) => Or(x, y)
      case _ => throw new RuntimeException("Must define min or max value.")
    }
    logger.debug(s"Expr: $ret")
    ret
  }

  private def checkMinLessThanMax(values: List[Json]): Unit = {

    if (values.forall(_.isNumber)) {
      values.flatMap(_.asNumber) match {
        case mv :: xv :: Nil if mv.toDouble >= xv.toDouble =>
          addEvent(ValidatorError(s"Min: ${minValue.get} must be less than max: ${maxValue.get}"))
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

    val values = (minValue :: maxValue :: Nil).flatten
    if (values.isEmpty) {
      addEvent(ValidatorError("Must defined minValue or maxValue or both."))
    }

    checkMinLessThanMax(values)

    val colType = findColumnInDataFrame(df, column)
    if (colType.isDefined) {
      val dataType = colType.get.dataType

      if (values.map(c => checkValue(df.schema, column, dataType, c)).exists(x => x)) {
        addEvent(ValidatorError(s"Range constraint types not compatible with column[$dataType]:'$column'"))
      }
    }

    if (inclusive.isDefined && inclusive.get.asBoolean.isEmpty) {
      logger.error(s"Inclusive defined but not Bool, $inclusive")
      addEvent(ValidatorError(s"Inclusive flag is defined, but is not a boolean, inclusive: ${inclusive.get}"))
    }

    failed
  }

  override def toJson: Json = {
    import JsonEncoders.eventEncoder
    val fields = Seq(
      ("type", Json.fromString("rangeCheck")),
      ("column", Json.fromString(column))
    ) ++
      minValue.map(mv => ("minValue", mv)) ++
      maxValue.map(mv => ("maxValue", mv)) ++
      Seq(
        ("inclusive", Json.fromBoolean(inclusive.flatMap(_.asBoolean).getOrElse(false))),
        ("events", getEvents.asJson)
      )
    Json.obj(fields: _*)
  }
}

object RangeCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val minValueJ = c.downField("minValue").as[Json].right.toOption
    val maxValueJ = c.downField("maxValue").as[Json].right.toOption
    val inclusiveJ = c.downField("inclusive").as[Json].right.toOption
    val threshold = c.downField("threshold").as[String].right.toOption

    logger.debug(s"column: $column")
    logger.debug(s"minValue: $minValueJ type: ${minValueJ.getClass.getCanonicalName}")
    logger.debug(s"maxValue: $maxValueJ type: ${maxValueJ.getClass.getCanonicalName}")
    logger.debug(s"inclusive: $inclusiveJ type: ${inclusiveJ.getClass.getCanonicalName}")
    logger.debug(s"threshold: $threshold")

    c.focus.foreach { f => logger.debug(s"RangeCheckJson: ${f.spaces2}") }
    scala.util.Right(RangeCheck(column, minValueJ, maxValueJ, inclusiveJ, threshold))
  }
}
