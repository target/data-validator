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

import scala.collection.mutable.ListBuffer
import scala.math.abs

abstract class ColumnBased(column: String, condTest: Expression) extends CheapCheck {
  override def select(schema: StructType, dict: VarSubstitution): Expression = condTest

  // ColumnBased checks don't have per row error details.
  def hasQuickErrorDetails: Boolean = false

  // calculates and returns the pct error as a string
  def calculatePctError(expected: Double, actual: Double, formatStr: String = "%4.2f%%"): String = {
    val pct = abs(((expected - actual) * 100.0) / expected)
    formatStr.format(pct)
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
    val pctError = if (failed) calculatePctError(minNumRows, count) else "0.00"
    addEvent(ValidatorCounter("rowCount", count))
    val msg = s"MinNumRowsCheck Expected: ${minNumRows} Actual: ${count} Error %: ${pctError}"
    val data = List(("Expected", minNumRows.toString), ("Actual", count.toString), ("Error Pct", pctError))
    addEvent(ColumnBasedValidatorCheckEvent(failed, data, msg))
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

  // scalastyle:off
  override def quickCheck(row: Row, count: Long, idx: Int): Boolean = {
    val dataType = row.schema(idx).dataType
    val rMax = row(idx)
    logger.info(s"rMax: $rMax colType: $dataType value: $value valueClass: ${value.getClass.getCanonicalName}")

    var pctError = "0.00"
    var errorMsg = ""
    val data = new ListBuffer[Tuple2[String, String]]

    failed = dataType match {
      case StringType => {
        val expected = value.asString.getOrElse("")
        val actual = row.getString(idx)
        data.appendAll(List(("Expected", expected), ("Actual", actual)))
        errorMsg = s"ColumnMaxCheck $column[$dataType]: Expected: $expected, Actual: $actual"
        expected != actual
      }
      case d:NumericType => {
        val num = value.asNumber.get
        var expected = 0.0
        var actual = 0.0
        d match {
          case ByteType =>
            expected = num.toByte.getOrElse[Byte](-1)
            actual = row.getByte(idx)
          case ShortType =>
            expected = num.toShort.getOrElse[Short](-1)
            actual = row.getShort(idx)
          case IntegerType =>
            expected = num.toInt.getOrElse[Int](-1)
            actual = row.getInt(idx)
          case LongType =>
            expected = num.toLong.getOrElse[Long](-1)
            actual = row.getLong(idx)
          case FloatType =>
            expected = num.toDouble
            actual = row.getFloat(idx)
          case DoubleType =>
            expected = num.toDouble
            actual = row.getDouble(idx)
        }
        pctError = if(expected != actual) calculatePctError(expected, actual) else "0.00"
        data.appendAll(List(("Expected", num.toString), ("Actual", rMax.toString), ("Error Pct", pctError)))
        errorMsg = s"ColumnMaxCheck $column[$dataType]: Expected: $num, Actual: $rMax. Error %: ${pctError}"
        expected != actual
      }
      case ut => {
        logger.error(s"ColumnMaxCheck for type: $ut, Row: $row not implemented! Please file this as a bug.")
        errorMsg = s"ColumnMaxCheck is not supported for data type ${dataType}"
        true // Fail check!
      }
    }
    logger.debug(s"MaxValue compared Row: $row with value: $value failed: $failed")
    if (failed) {
      addEvent(ColumnBasedValidatorCheckEvent(failed, data.toList, errorMsg))
    }
    failed
  }
  // scalastyle:on

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("columnMaxCheck")),
    ("column", Json.fromString(column)),
    ("value", value),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )
}
