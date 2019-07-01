package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorCounter, ValidatorError, VarSubstitution}
import com.target.data_validator.JsonEncoders.eventEncoder
import io.circe.Json
import io.circe.syntax._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.types._

abstract class ColumnBased(column: String, condTest: Expression) extends CheapCheck {
  override def select(schema: StructType, dict: VarSubstitution): Expression = condTest

  // ColumnBased checks don't have per row error details.
  def hasQuickErrorDetails: Boolean = false
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
    addEvent(ValidatorCounter("rowCount", count))
    addEvent(ValidatorCheckEvent(failed, s"MinNumRowCheck $minNumRows ", count, 1))
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
    val num = value.asNumber
    failed = dataType match {
      case StringType => value.asString.exists(_ != row.getString(idx))
      case ByteType => num.map(_.toByte).exists(_.get != row.getByte(idx))
      case ShortType => num.map(_.toShort).exists(_.get != row.getShort(idx))
      case IntegerType =>
        val intNum = value.asNumber.map(_.toInt.get).getOrElse(-1)
        val rowInt = row.getInt(idx)
        logger.debug(s"intNum[${intNum.getClass.getCanonicalName}]: $intNum " +
          s"rowInt[${rowInt.getClass.getCanonicalName}]: $rowInt")
        num.map(_.toInt).exists(_.get != row.getInt(idx))
      case LongType => num.map(_.toLong).exists(_.get != row.getLong(idx))
      case FloatType => num.forall(_.toDouble != row.getFloat(idx))
      case DoubleType => num.forall(_.toDouble != row.getDouble(idx))
      case ut =>
        logger.error(s"quickCheck for type: $ut, Row: $row not Implemented! Please file this as a bug.")
        true // Fail check!
    }
    logger.debug(s"MaxValue compared Row: $row with value: $value failed: $failed")
    if (failed) {
      addEvent(
        ValidatorCheckEvent(
          failed,
          s"columnMaxCheck column[$dataType]: $column value: $value doesn't equal $rMax",
          count,
          1
        )
      )
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
