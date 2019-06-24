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
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

case class StringLengthCheck(
                       column: String,
                       minValue: Option[Json],
                       maxValue: Option[Json]
                     ) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {

    val ret = StringLengthCheck(
                                  getVarSub(column, "column", dict),
                                  minValue.map(getVarSubJson(_, "minValue", dict)),
                                  maxValue.map(getVarSubJson(_, "maxValue", dict))
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

    val minValueExpression = cmpExpr(colExp, minValue, LessThan)
    val maxValueExpression = cmpExpr(colExp, maxValue, GreaterThan)

    val ret = (minValueExpression, maxValueExpression) match {
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
            addEvent(ValidatorError(s"min: ${minValue.get} must be less than or equal to max: ${maxValue.get}"))
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
    val values = (minValue::maxValue::Nil).flatten
    if (values.isEmpty) {
      addEvent(ValidatorError("Must define minValue or maxValue or both."))
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
      minValue.map(mv => ("minValue", mv)) ++
      maxValue.map(mv => ("maxValue", mv)) ++
      Seq(
        ("events", getEvents.asJson)
      )
    Json.obj(fields: _*)
  }
}

object StringLengthCheck extends LazyLogging {
  def fromJson(c: HCursor): Either[DecodingFailure, ValidatorBase] = {
    val column = c.downField("column").as[String].right.get
    val minValueJ = c.downField("minValue").as[Json].right.toOption
    val maxValueJ = c.downField("maxValue").as[Json].right.toOption

    logger.debug(s"column: $column")
    logger.debug(s"minValue: $minValueJ type: ${minValueJ.getClass.getCanonicalName}")
    logger.debug(s"maxValue: $maxValueJ type: ${maxValueJ.getClass.getCanonicalName}")

    c.focus.foreach {f => logger.info(s"StringLengthCheckJson: ${f.spaces2}")}
    scala.util.Right(StringLengthCheck(column, minValueJ, maxValueJ))
  }
}
