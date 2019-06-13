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

abstract class RowBased extends ValidatorBase {

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

}

