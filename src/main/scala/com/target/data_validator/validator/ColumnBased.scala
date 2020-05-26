package com.target.data_validator.validator

import com.target.data_validator.{ColumnBasedValidatorCheckEvent, ValidatorCounter, ValidatorError, VarSubstitution}
import com.target.data_validator.JsonEncoders.eventEncoder
import io.circe.Json
import io.circe.syntax._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.types._

import scala.collection.mutable.ListMap
import scala.math.abs

abstract class ColumnBased(column: String, condTest: Expression) extends CheapCheck {
  override def select(schema: StructType, dict: VarSubstitution): Expression = condTest

  // ColumnBased checks don't have per row error details.
  def hasQuickErrorDetails: Boolean = false

  // calculates and returns the pct error as a string
  def calculatePctError(expected: Double, actual: Double, formatStr: String = "%4.2f%%"): String = {

    if (expected == actual) {
      formatStr.format(0.00) // if expected == actual, error % should be 0, even if expected is 0
    }
    else if (expected == 0.0) {
      "undefined"
    }
    else {
      val pct = abs(((expected - actual) * 100.0) / expected)
      formatStr.format(pct)
    }
  }
}

case class MinNumRows(minNumRows: Long) extends ColumnBased("", ValidatorBase.L0) {
  override def name: String = "MinNumRows"

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = this

  override def configCheck(df: DataFrame): Boolean = {
    if (minNumRows <= 0) {
      val msg = s"MinNumRows: $minNumRows <= 0"
      logger.error(msg)
      addEvent(ValidatorError(msg))
      failed = true
      true
    } else {
      false
    }
  }

  override def quickCheck(row: Row, count: Long, idx: Int): Boolean = {
    failed = count < minNumRows
    val pctError = if (failed) calculatePctError(minNumRows, count) else "0.00%"
    addEvent(ValidatorCounter("rowCount", count))
    val msg = s"MinNumRowsCheck Expected: $minNumRows Actual: $count Relative Error: $pctError"
    val data = ListMap("expected" -> minNumRows.toString, "actual" -> count.toString,
                             "relative_error" -> pctError)
    addEvent(ColumnBasedValidatorCheckEvent(failed, data.toMap, msg))
    failed
  }

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("rowCount")),
    ("minNumRows", Json.fromLong(minNumRows)),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )

  override def toString: String = name + s"(minNumRows: $minNumRows)"
}

case class ColumnMaxCheck(column: String, value: Json)
  extends ColumnBased(column, Max(UnresolvedAttribute(column)).toAggregateExpression()) {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = copy(column = getVarSub(column, "column", dict), value = getVarSubJson(value, "value", dict))
    this.getEvents.foreach(ret.addEvent)
    ret
  }

  override def configCheck(df: DataFrame): Boolean = checkTypes(df, column, value)

  override def quickCheck(row: Row, count: Long, idx: Int): Boolean = {
    val dataType = row.schema(idx).dataType
    val rMax = row(idx)
    logger.info(s"rMax: $rMax colType: $dataType value: $value valueClass: ${value.getClass.getCanonicalName}")

    var errorMsg = ""
    val data = ListMap.empty[String, String]

    def resultForString(): Unit = {
      val (expected, actual) = (value.asString.getOrElse(""), row.getString(idx))

      failed = expected != actual
      data += ("expected" -> expected, "actual" -> actual)
      errorMsg = s"ColumnMaxCheck $column[StringType]: Expected: $expected, Actual: $actual"
    }

    def resultForNumeric(): Unit = {
      val num = value.asNumber.get
      var cmp_params = (0.0, 0.0) // (expected, actual)

      dataType match {
        case ByteType => cmp_params = (num.toByte.getOrElse[Byte](-1), row.getByte(idx))
        case ShortType => cmp_params = (num.toShort.getOrElse[Short](-1), row.getShort(idx))
        case IntegerType => cmp_params = (num.toInt.getOrElse[Int](-1), row.getInt(idx))
        case LongType => cmp_params = (num.toLong.getOrElse[Long](-1), row.getLong(idx))
        case FloatType => cmp_params = (num.toDouble, row.getFloat(idx))
        case DoubleType => cmp_params = (num.toDouble, row.getDouble(idx))
      }

      failed = cmp_params._1 != cmp_params._2
      val pctError = if (failed) calculatePctError(cmp_params._1, cmp_params._2) else "0.00%"
      data += ("expected" -> num.toString, "actual" -> rMax.toString, "relative_error" -> pctError)
      errorMsg = s"ColumnMaxCheck $column[$dataType]: Expected: $num, Actual: $rMax. Relative Error: ${pctError}"
    }

    def resultForOther(): Unit = {
      logger.error(
        s"""ColumnMaxCheck for type: $dataType, Row: $row not implemented!
           |Please open a bug report on the data-validator issue tracker.""".stripMargin
      )
      failed = true
      errorMsg = s"ColumnMaxCheck is not supported for data type $dataType"
    }

    dataType match {
      case StringType => resultForString()
      case _: NumericType => resultForNumeric()
      case _ => resultForOther()
    }

    logger.debug(s"MaxValue compared Row: $row with value: $value failed: $failed")
    if (failed) {
      addEvent(ColumnBasedValidatorCheckEvent(failed, data.toMap, errorMsg))
    }
    failed
  }

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("columnMaxCheck")),
    ("column", Json.fromString(column)),
    ("value", value),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )
}
