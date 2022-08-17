package com.target.data_validator.validator

import com.target.data_validator._
import com.target.data_validator.validator.ValidatorBase.{isColumnInDataFrame, L0, L1}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

abstract class RowBased extends CheapCheck {

  val column: String
  val threshold: Option[String]

  def configCheck(df: DataFrame): Boolean = {
    configCheckColumn(df)
    configCheckThreshold
    failed
  }

  def configCheckColumn(df: DataFrame): Boolean = {
    if (isColumnInDataFrame(df, column)) {
      logger.debug(s"Column: $column found in table.")
      false
    } else {
      val msg = s"Column: $column not found in schema."
      logger.error(msg)
      addEvent(ValidatorError(msg))
      failed
    }
  }

  def configCheckThreshold: Boolean = {
    if (threshold.isDefined) {
      val ret = threshold.flatMap(RowBased.THRESHOLD_NUMBER_REGEX.findFirstIn).isEmpty
      if (ret) {
        val msg = s"Threshold `${threshold.get}` not parsable."
        logger.error(msg)
        addEvent(ValidatorError(msg))
      }
      ret
    } else {
      false
    }
  }

  def colTest(schema: StructType, dict: VarSubstitution): Expression

  def select(schema: StructType, dict: VarSubstitution): Expression = If(colTest(schema, dict), L1, L0)

  /** Calculates the max acceptable number of errors from threshold and rowCount.
    * @param rowCount
    *   of table.
    * @return
    *   max number of errors we can tolerate. if threshold < 1, then its a percentage of rowCount. if threshold ends
    *   with '%' then its percentage of rowCount if threshold is > 1, then its maxErrors.
    */
  def calcErrorCountThreshold(rowCount: Long): Long = {
    threshold
      .map { t =>
        val tempThreshold = t.stripSuffix("%").toDouble
        val ret: Long = if (t.endsWith("%")) {
          // Has '%', so divide by 100.0
          (tempThreshold * (rowCount / 100.0)).toLong
        } else if (tempThreshold < 1.0) {
          // Percentage without the '%'
          (tempThreshold * rowCount).toLong
        } else {
          // Number of rows
          tempThreshold.toLong
        }
        logger.info(s"Threshold:${threshold.get} tempThreshold:$tempThreshold ret:$ret")
        ret
      }
      .getOrElse(0)
  }

  override def quickCheck(row: Row, count: Long, idx: Int): Boolean = {
    logger.debug(s"quickCheck $column Row: $row count: $count idx: $idx")
    if (count > 0) {
      val errorCount = row.getLong(idx)
      val errorCountThreshold = calcErrorCountThreshold(count)

      addEvent(ValidatorCounter("rowCount", count))
      addEvent(ValidatorCounter("errorCount", errorCount))
      if (errorCountThreshold > 0) {
        addEvent(ValidatorCounter("errorCountThreshold", errorCountThreshold))
      }

      val failure = errorCount > errorCountThreshold
      if (failure) {
        logger.error(
          s"Quick check for $name on $column failed, $errorCount errors in $count rows"
            + s" errorCountThreshold: $errorCountThreshold"
        )
      }
      addEvent(ValidatorCheckEvent(failure, s"$name on column '$column'", count, errorCount))
    } else {
      logger.warn(s"No Rows to check for $toString!")
    }
    failed
  }

  def quickCheckDetail(row: Row, key: Seq[(String, Any)], idx: Int, dict: VarSubstitution): Unit = {
    val r = row.get(idx)
    val column = row.schema.fieldNames(idx)
    addEvent(
      ValidatorQuickCheckError(key.toList, r, name + s" failed! $column = $r and ${colTest(row.schema, dict)}")
    )
  }
}

object RowBased {
  val THRESHOLD_NUMBER_REGEX: Regex = "^([0-9]+\\.*[0-9]*)\\s*%{0,1}$".r // scalastyle:ignore
}
