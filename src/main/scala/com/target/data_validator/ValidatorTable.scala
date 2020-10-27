package com.target.data_validator

import com.target.data_validator.validator.{CheapCheck, ColumnBased, CostlyCheck, RowBased, ValidatorBase}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scalatags.Text.all._

abstract class ValidatorTable(
  keyColumns: Option[Seq[String]],
  condition: Option[String],
  checks: List[ValidatorBase],
  label: String,
  events: ListBuffer[ValidatorEvent] = new ListBuffer[ValidatorEvent]
) extends Substitutable {

  def getDF(session: SparkSession): Try[DataFrame]

  def open(session: SparkSession): Try[DataFrame] = getDF(session).map(condition.foldLeft(_)((d, c) => d.where(c)))

  def createKeySelect(df: DataFrame)(implicit vc: ValidatorConfig): Seq[String] = {
    val ret: Seq[String] = keyColumns.getOrElse(df.columns.take(vc.numKeyCols))
    logger.error(s"createKeySelect: ${ret.mkString(", ")} keyColumns: $keyColumns")
    ret
  }

  def numKeyColumns()(implicit vc: ValidatorConfig): Int = keyColumns.map(_.length).getOrElse(vc.numKeyCols)

  def createCountSelect(): Seq[Column] =
    new Column(Alias(Count(ValidatorBase.L1).toAggregateExpression(), "count")()) :: Nil

  def checkKeyCols(df: DataFrame): Boolean = {
    keyColumns.exists { cols =>
      val fieldSet = df.schema.fields.map(_.name).toSet
      cols.forall { c =>
        val found = fieldSet(c)
        if (!found) {
          val errMsg = s"KeyColumn '$c' is not a column in $label"
          logger.error(errMsg)
          addEvent(ValidatorError(errMsg))
        }
        !found
      }
    }
  }

  def configCheck(session: SparkSession, dict: VarSubstitution): Boolean = {
    logger.debug(s"configCheck $label")
    val timer = new ValidatorTimer(s"configCheck for $label")
    addEvent(timer)
    val ret = timer.time {
      open(session) match {
        case Success(df) =>
          val error = (List(checkKeyCols(df)) ++ checks.map(_.configCheck(df))).exists(x => x)
          if (error) {
            val msg = s"ConfigCheck failed for $label"
            logger.error(msg)
            addEvent(ValidatorError(msg))
          }  else {
            addEvent(ValidatorGood(s"ConfigCheck for $label"))
          }
          error
        case Failure(e) =>
          logger.error(s"Failed to open table $label ${e.getMessage}")
          addEvent(ValidatorError(s"Failed to open table '$label'"))
          true
      }
    }
    logger.debug(s"ConfigCheck ret: $ret")
    ret
  }

  def quickChecks(session: SparkSession, dict: VarSubstitution)(implicit vc: ValidatorConfig): Boolean = {
    val dataFrame = open(session).get
    val qc: List[CheapCheck] = checks.flatMap {
      case cc: CheapCheck => Some(cc)
      case _ => None
    }
    val checkSelects: Seq[Expression] = qc.map {
      case colChk: ColumnBased => colChk.select(dataFrame.schema, dict)
      case chk: RowBased => Sum(chk.select(dataFrame.schema, dict)).toAggregateExpression()
    }

    val cols: Seq[Column] = createCountSelect() ++ checkSelects.zipWithIndex.map {
      case (chkSelect: Expression, idx: Int) => new Column(Alias(chkSelect, s"qc$idx")())
    }

    logger.debug(s"quickChecks $label select: $cols")

    val timer = new ValidatorTimer(s"$label: quickCheck timer")
    addEvent(timer)
    val countDf = timer.time {
      dataFrame.select(cols: _*).take(1)
    }
    val results = countDf.head
    logger.info(s"Results: $results")
    val count = results.getLong(0)

    logger.info(s"Total Rows Processed: $count")
    addEvent(ValidatorCounter(s"RowCount for $label", count))

    val failed = qc.zipWithIndex.map {
      case (check: CheapCheck, idx: Int) => check.quickCheck(results, count, idx + 1)
    }.exists(x => x)

    if (failed) {
      val failedChecks = checks.filter(_.failed).map(_.toString).mkString(", ")
      addEvent(ValidatorError(s"QuickChecks on $label failed for $failedChecks"))
      if (vc.detailedErrors) {
        quickErrorDetails(dataFrame, dict)
      }
    }
    failed
  }

  def costlyChecks(session: SparkSession, dict: VarSubstitution)(implicit  vc: ValidatorConfig): Boolean = {
    val df = open(session).get
    val cc = checks.flatMap {
      case cc: CostlyCheck => Some(cc)
      case _ => None
    }
    cc.map(_.costlyCheck(df)).exists(x => x)
  }

  def quickErrorDetails(dataFrame: DataFrame, dict: VarSubstitution)(implicit vc: ValidatorConfig): Unit = {
    val keySelect = createKeySelect(dataFrame)
    val failedChecksWithIndex = checks
      .filter(c => c.failed && c.isInstanceOf[RowBased])
      .map(_.asInstanceOf[RowBased])
      .zipWithIndex
    if (failedChecksWithIndex.nonEmpty) {
      val detailedErrorSelect = failedChecksWithIndex.map {
        case (chk, _) => chk.column
      }
      val failedChecksCondition = failedChecksWithIndex.map {
        case (chk, _) => chk.colTest(dataFrame.schema, dict)
      }
      val selects = keySelect ++ detailedErrorSelect
      val condition = new Column(ExpressionUtils.orFromList(failedChecksCondition))

      logger.debug(s"Table: $label")
      logger.debug(s"Selects: $selects")
      logger.debug(s"condition: $condition")

      val timer = new ValidatorTimer(s"quickErrorDetails for $label")
      addEvent(timer)
      val errorsDf = timer.time {
        dataFrame.selectExpr(selects: _*).filter(condition).take(vc.numErrorsToReport)
      }

      for (r <- errorsDf; (failedCheck, idx) <- failedChecksWithIndex) {
        failedCheck.quickCheckDetail(r, ValidatorTable.buildKey(numKeyColumns(), r), idx + numKeyColumns(), dict)
      }
    }
  }

  def addEvent(ev: ValidatorEvent): Unit = events.append(ev)
  def getEvents: List[ValidatorEvent] = events.toList

  def failed: Boolean = events.exists(_.failed || checks.exists(_.failed))

  def generateHTMLReport: Tag = div(cls:="validatorTable")(
    h2(label),
    div(id := "validator_report",
      checks.map(_.generateHTMLReport())),
    hr())

  def substituteVariables(dict: VarSubstitution): ValidatorTable
}

//
// Used with Hive
//
case class ValidatorHiveTable(
  db: String,
  table: String,
  useHWC: Option[Boolean],
  keyColumns: Option[List[String]],
  condition: Option[String],
  checks: List[ValidatorBase]
) extends ValidatorTable(
  keyColumns,
  condition,
  checks,
  s"HiveTable:`$db.$table`" + condition.map(c => s" with condition($c)").getOrElse("")
) {

  override def getDF(session: SparkSession): Try[DataFrame] = {
    logger.info(s"Opening table: $db.$table")
    useHWC match {
      case Some(x) if x =>
        val hive = com.hortonworks.hwc.HiveWarehouseSession.session(session).build()
        val query = condition match {
          case Some(c) => s"SELECT * FROM $db.$table WHERE $c"
          case None => s"SELECT * FROM $db.$table"
        }
        Try(hive.executeQuery(query))
      case _ =>
        Try(session.table(s"$db.$table"))
    }

  }

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newDb = getVarSub(db, "db", dict)
    val newTable = getVarSub(table, "table", dict)
    val newKeyColumns = keyColumns.map(v => v.map(x => getVarSub(x, "keyCol", dict)))
    val newCondition = condition.map(x => getVarSub(x, "condition", dict))
    val newChecks = checks.map(_.substituteVariables(dict))

    val ret = ValidatorHiveTable(newDb, newTable, useHWC, newKeyColumns, newCondition, newChecks)
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

//
// Used with Orc files
//
case class ValidatorOrcFile(
  orcFile: String,
  keyColumns: Option[List[String]],
  condition: Option[String],
  checks: List[ValidatorBase]
) extends ValidatorTable(
  keyColumns,
  condition,
  checks,
  "OrcFile:" + orcFile + condition.map(c => s" with Condition($c)").getOrElse("")
) {
  override def getDF(session: SparkSession): Try[DataFrame] = {
    logger.info(s"Reading orc file: $orcFile")
    Try(session.read.orc(orcFile))
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newOrcFile = getVarSub(orcFile, "orcFile", dict)
    val newKeyColumns = keyColumns.map(v => v.map(x => getVarSub(x, "keyCol", dict)))
    val newCondition = condition.map(x => getVarSub(x, "condition", dict))
    val newChecks = checks.map(_.substituteVariables(dict))

    val ret = ValidatorOrcFile(newOrcFile, newKeyColumns, newCondition, newChecks)
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

case class ValidatorParquetFile(
  parquetFile: String,
  keyColumns: Option[List[String]],
  condition: Option[String],
  checks: List[ValidatorBase]
) extends ValidatorTable(
  keyColumns,
  condition,
  checks,
  "ParquetFile:" + parquetFile + condition.map(c => s" with Condition($c)").getOrElse("")
) {
  override def getDF(session: SparkSession): Try[DataFrame] = {
    logger.info(s"Reading parquet file: $parquetFile")
    Try(session.read.parquet(parquetFile))
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newParquetFile = getVarSub(parquetFile, "parquetFile", dict)
    val newKeyColumns = keyColumns.map(v => v.map(x => getVarSub(x, "keyCol", dict)))
    val newCondition = condition.map(x => getVarSub(x, "condition", dict))
    val newChecks = checks.map(_.substituteVariables(dict))

    val ret = ValidatorParquetFile(newParquetFile, newKeyColumns, newCondition, newChecks)
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

//
// Used for testing.
//
case class ValidatorDataFrame(
  df: DataFrame,
  keyColumns: Option[Seq[String]],
  condition: Option[String],
  checks: List[ValidatorBase]
) extends ValidatorTable(
  keyColumns,
  condition,
  checks,
  "DataFrame" + condition.map(x => s" with condition($x)").getOrElse("")
) {
  override def getDF(session: SparkSession): Try[DataFrame] = Success(df)

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newKeyColumns = keyColumns.map(v => v.map(x => getVarSub(x, "keyCol", dict)))
    val newCondition = condition.map(x => getVarSub(x, "condition", dict))
    val newChecks = checks.map(_.substituteVariables(dict))

    val ret = ValidatorDataFrame(df, newKeyColumns, newCondition, newChecks)
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

object ValidatorTable {
  def buildKey(n: Int, r: Row): Seq[(String, Any)] = Range(0, n).map(x => (r.schema(x).name, r(x)))
}
