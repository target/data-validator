package com.target.data_validator

import com.target.data_validator.validator._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Sum}

import scala.collection.mutable.ListBuffer
import scala.util._
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
    logger.info(s"keyColumns: ${ret.mkString(", ")}")
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

  private def performFirstPass(df: DataFrame, checks: List[TwoPassCheapCheck]): Unit = {
    if (checks.nonEmpty) {
      val firstPassTimer = new ValidatorTimer(s"$label: pre-processing stage")

      addEvent(firstPassTimer)

      firstPassTimer.time {
        val cols = checks.map { _.firstPassSelect() }
        val row = df.select(cols: _*).head

        checks foreach { _ sinkFirstPassRow row }
      }
    }
  }

  private def cheapExpression(dataFrame: DataFrame, dict: VarSubstitution): PartialFunction[CheapCheck, Expression] = {
    case tp: TwoPassCheapCheck => tp.select(dataFrame.schema, dict)
    case colChk: ColumnBased => colChk.select(dataFrame.schema, dict)
    case chk: RowBased => Sum(chk.select(dataFrame.schema, dict)).toAggregateExpression()
  }

  def quickChecks(session: SparkSession, dict: VarSubstitution)(implicit vc: ValidatorConfig): Boolean = {
    val dataFrame = open(session).get

    performFirstPass(dataFrame, checks.collect { case tp: TwoPassCheapCheck => tp })

    val qc: List[CheapCheck] = checks.flatMap {
      case cc: CheapCheck => Some(cc)
      case _ => None
    }
    val checkSelects = qc.map(cheapExpression(dataFrame, dict))

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

  /**
   * Most extenders of `ValidatorTable` will substitute these but it's left optional by the API.
   *
   * @param dict the substitution dictionary
   * @return a simple data class with the new key columns, condition, and checks
   */
  protected def substituteKeyColsCondsChecks(dict: VarSubstitution): SubstitutedKeyColsCondChecks = {
    val newKeyColumns = keyColumns.map(v => v.map(x => getVarSub(x, "keyCol", dict)))
    val newCondition = condition.map(x => getVarSub(x, "condition", dict))
    val newChecks = checks.map(_.substituteVariables(dict))

    new SubstitutedKeyColsCondChecks(newKeyColumns, newCondition, newChecks)
  }

  class SubstitutedKeyColsCondChecks(
    val keyColumns: Option[Seq[String]],
    val condition: Option[String],
    val checks: List[ValidatorBase]
  )
}

/**
 * Load a database table from Hive using the default Hive warehouse adapter
 *
 * @param db the database
 * @param table the table to load
 */
case class ValidatorHiveTable(
  db: String,
  table: String,
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
    Try(session.table(s"$db.$table"))
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newDb = getVarSub(db, "db", dict)
    val newTable = getVarSub(table, "table", dict)
    val newBases = substituteKeyColsCondsChecks(dict)

    val ret = ValidatorHiveTable(
      newDb, newTable,
      newBases.keyColumns.map(_.toList),
      newBases.condition,
      newBases.checks
    )
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

/**
 * Enables usage of the normal `spark.read.format(String).options(Map[String,String]).load()` fluent API
 * for creating a [[DataFrame]] using a dynamic loader.
 *
 * For reading well-known formats such as Parquet or ORC,
 * use [[ValidatorParquetFile]] or [[ValidatorOrcFile]], respectively.
 * Under the hood, these just call `spark.read.format("orc")` but let's let Spark handle that.
 *
 * @param format the format that the [[DataFrameReader]] should use
 * @param options options that the [[DataFrameReader]] should use
 * @param loadData If present, this will passed to [[DataFrameReader.load(String)]],
 *                 otherwise the DFR will use the parameterless version
 */
case class ValidatorSpecifiedFormatLoader(
  format: String,
  keyColumns: Option[List[String]],
  condition: Option[String],
  checks: List[ValidatorBase],
  options: Option[Map[String, String]] = None,
  loadData: Option[List[String]] = None
) extends ValidatorTable(
  keyColumns,
  condition,
  checks,
  label = s"""CustomFormat:$format${condition.map(c => s" with Condition($c)").getOrElse("")}"""
) {
  override def getDF(session: SparkSession): Try[DataFrame] = {
    logger.info(s"Reading using custom format ${format} with options ${options}")
    if (loadData.isDefined) {
      logger.debug(s"${format} loading with ${loadData}")
    }

    Try {
      val optionedReader = session.read.format(format).options(options.getOrElse(Map.empty))
      // TODO: support the varargs version of load()
      loadData.fold[DataFrame](ifEmpty = optionedReader.load())(data => optionedReader.load(data: _*))
    }
  }

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newFormat = getVarSub(format, "format", dict)
    val newLoadData = loadData.map(_.map(getVarSub(_, "loadData", dict)))
    val newOptions = options.map { _.map {
      case (optKey, optVal) => optKey -> getVarSub(optVal, optKey, dict)
    } }

    val newBases = substituteKeyColsCondsChecks(dict)

    val ret = ValidatorSpecifiedFormatLoader(
      newFormat,
      newBases.keyColumns.map(_.toList),
      newBases.condition,
      newBases.checks,
      newOptions,
      newLoadData
    )
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
    val newBases = substituteKeyColsCondsChecks(dict)

    val ret = ValidatorOrcFile(
      newOrcFile,
      newBases.keyColumns.map(_.toList),
      newBases.condition,
      newBases.checks
    )
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

    val newBases = substituteKeyColsCondsChecks(dict)

    val ret = ValidatorParquetFile(
      newParquetFile,
      newBases.keyColumns.map(_.toList),
      newBases.condition,
      newBases.checks
    )
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

  final def label: String = "DataFrame" + condition.map(x => s" with condition($x)").getOrElse("")

  override def getDF(session: SparkSession): Try[DataFrame] = Success(df)

  override def substituteVariables(dict: VarSubstitution): ValidatorTable = {
    val newBases = substituteKeyColsCondsChecks(dict)

    val ret = ValidatorDataFrame(
      df,
      newBases.keyColumns.map(_.toList),
      newBases.condition,
      newBases.checks
    )
    this.getEvents.foreach(ret.addEvent)
    ret
  }
}

object ValidatorTable {
  def buildKey(n: Int, r: Row): Seq[(String, Any)] = Range(0, n).map(x => (r.schema(x).name, r(x)))
}
