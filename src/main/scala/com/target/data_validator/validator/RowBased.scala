package com.target.data_validator.validator

import com.target.data_validator.{ValidatorCheckEvent, ValidatorError, ValidatorQuickCheckError, VarSubstitution}
import com.target.data_validator.JsonEncoders.eventEncoder
import com.target.data_validator.validator.ValidatorBase.{isColumnInDataFrame, I0, L0, L1}
import io.circe.Json
import io.circe.syntax._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{NumericType, StructType}

abstract class RowBased extends CheapCheck {

  val column: String

  def configCheck(df: DataFrame): Boolean = configCheckColumn(df)

  def configCheckColumn(df: DataFrame): Boolean = {
    if (isColumnInDataFrame(df, column)) {
      logger.debug(s"Column: $column found in table.")
      false
    } else {
      val msg = s"Column: $column not found in table."
      logger.error(msg)
      addEvent(ValidatorError(msg))
      failed
    }
  }

  def colTest(schema: StructType, dict: VarSubstitution): Expression

  def select(schema: StructType, dict: VarSubstitution): Expression = If(colTest(schema, dict), L1, L0)

  override def quickCheck(row: Row, count: Long, idx: Int): Boolean = {
    logger.debug(s"quickCheck $column Row: $row count: $count idx: $idx")
    if (count > 0) {
      val errorCount = row.getLong(idx)
      val failure = errorCount > 0
      if (failure) logger.error(s"Quick check for $name on $column failed, $errorCount errors in $count rows.")
      addEvent(ValidatorCheckEvent(failure, s"$name on column '$column'", count, errorCount))
    } else {
      logger.warn(s"No Rows to check for $toString!")
    }
    failed
  }

  def quickCheckDetail(row: Row, key: Seq[(String, Any)], idx: Int, dict: VarSubstitution): Unit = {
    val r = row.get(idx)
    val column = row.schema.fieldNames(idx)
    addEvent(ValidatorQuickCheckError(key.toList, r, name + s" failed! $column = $r and ${colTest(row.schema, dict)}"))
  }
}

case class NullCheck(column: String) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = NullCheck(getVarSub(column, "column", dict))
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

case class NegativeCheck(column: String) extends RowBased {

  override def substituteVariables(dict: VarSubstitution): ValidatorBase = {
    val ret = NegativeCheck(getVarSub(column, "column", dict))
    getEvents.foreach(ret.addEvent)
    ret
  }

  override def configCheck(df: DataFrame): Boolean = {
    findColumnInDataFrame(df, column) match {
      case Some(ft) if ft.dataType.isInstanceOf[NumericType] => Unit
      case Some(ft) =>
        val msg = s"Column: $column found, but not of numericType type: ${ft.dataType}"
        logger.error(msg)
        addEvent(ValidatorError(msg))
      case None =>
        val msg = s"Column: $column not found in schema."
        logger.error(msg)
        addEvent(ValidatorError(msg))
    }
    failed
  }

  override def colTest(schema: StructType, dict: VarSubstitution): Expression =
    LessThan (UnresolvedAttribute(column), I0)

  override def toJson: Json = Json.obj(
    ("type", Json.fromString("negativeCheck")),
    ("column", Json.fromString(column)),
    ("failed", Json.fromBoolean(failed)),
    ("events", this.getEvents.asJson)
  )
}
