package com.target.data_validator.validator

import com.target.data_validator._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Json, JsonNumber}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.{TypeCoercion, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, _}

import scala.collection.mutable.ListBuffer
import scalatags.Text.all._

abstract class ValidatorBase(
  var failed: Boolean = false,
  events: ListBuffer[ValidatorEvent] = new ListBuffer[ValidatorEvent]
) extends Substitutable {
  import ValidatorBase._

  def name: String = this.getClass.getSimpleName

  def substituteVariables(dict: VarSubstitution): ValidatorBase

  def configCheck(df: DataFrame): Boolean

  def select(schema: StructType, dict: VarSubstitution): Expression

  def quickCheck(r: Row, count: Long, idx: Int): Boolean

  def generateHTMLReport: Tag = {
    val d = div(cls := "check_report")
    if (failed) {
      val failedEvents = events.filter(_.failed)
      d(failedEvents.map(_.toHTML): _*)
    } else {
      d(HTMLBits.pass, " - " + toString)
    }
  }

  def toJson: Json

  // Utility methods

  final def addEvent(ve: ValidatorEvent): Unit = {
    events.append(ve)
    failed ||= ve.failed
  }

  final def getEvents: List[ValidatorEvent] = events.toList

  /**
    * Check Types of column and value
    * @param value - what we are comparing to from config.
    * @return true on Error
    */
  private[validator] def checkTypes(df: DataFrame, column: String, value: Json): Boolean = {
    if (isColumnInDataFrame(df, column)) {
      logger.debug(s"Column: $column found in table.")
      val dataType = df.schema(column).dataType
      if (!areTypesCompatible(dataType, value)) {
        val msg = s"checkTypes failed for $name column[$dataType]: $column " +
          s"value[${value.getClass.getCanonicalName}]: $value"
        logger.error(msg)
        addEvent(ValidatorError(msg))
      }
    } else {
      val msg = s"Column: $column not found in table."
      logger.error(msg)
      addEvent(ValidatorError(msg))
    }
    failed
  }

  private[validator] def findColumnInDataFrame(dataFrame: DataFrame, column: String): Option[StructField] = {
    val ret = dataFrame.schema.fields.find(_.name == column)
    if (ret.isEmpty) {
      addEvent(ValidatorError(s"Column: $column not found in schema."))
    }
    ret
  }

  private[validator] def checkValueString(
    schema: StructType,
    column: String,
    colType: DataType,
    value: String
  ): Boolean = {
    val ret = if (isValueColumn(value)) {
      lookupValueColumn(schema, value) match {
        case None =>
          addEvent(ValidatorError(s"value: $value not found in schema."))
          true
        case Some(sf) if sf.name == column =>
          addEvent(ValidatorError(s"value: $value cannot be equal to column: $column"))
          true
        case Some(sf) if TypeCoercion.findWiderTypeForTwo(colType, sf.dataType).isEmpty =>
          addEvent(ValidatorError(s"value[${sf.dataType}]: $value not compatible with column[$colType]: $column"))
          true
        case Some(_) =>
          false
      }
    } else {
      // value is not a column, this shouldn't happen.
      logger.error(s"value: $value is not a column.")
      true
    }
    logger.debug(s"checkValueString: column[$colType]: $column value: $value ret: $ret")
    getEvents.foreach(event => logger.debug(s"Event: $event"))
    ret
  }

  /**
    * Checks a value for compatibility with column.
    * @param schema - from DataFrame.
    * @param column - to check.
    * @param colType - DataType of column.
    * @param value - Json value to compare against column, could be another column.
    * @return True on error.
    */
  private[validator] def checkValue(
    schema: StructType,
    column: String,
    colType: DataType,
    value: Json
  ): Boolean = {
    val ret = value match {
      case v: Json if v.isNumber => !areNumberTypesCompatible(colType, v.asNumber)
      case v: Json if v.isString => checkValueString(schema, column, colType, v.asString.get)
      case _ => true
    }
    logger.debug(s"checkValue: column[$colType]: $column value: ${JsonUtils.debugJson(value)} ret: $ret")
    ret
  }
}

object ValidatorBase extends LazyLogging {

  private val backtick = "`"
  val I0: Literal = Literal.create(0, IntegerType)
  val D0: Literal = Literal.create(0.0, DoubleType)
  val L0: Literal = Literal.create(0, LongType)
  val L1: Literal = Literal.create(1, LongType)

  def isValueColumn(v: String): Boolean = v.startsWith(backtick)

  def isValueColumn(json: Json): Boolean = json.asString.exists(isValueColumn)

  def lookupValueColumn(schema: StructType, value: String): Option[StructField] = if (isValueColumn(value)) {
    val vc = value.stripPrefix(backtick)
    val ret = schema.fields.find(_.name == vc)
    logger.debug(s"lookupValueColumn value: $value ret: $ret")
    ret
  } else {
    logger.debug(s"lookupValueColumn value: $value is not column.")
    None
  }

  /**
    * searches for a columnName in schema
    * @param schema - from source table.
    * @param columnName - value from config that could be reference to column.
    * @return True if v is found in schema.
    */
  def schemaContainsValueColumn(schema: StructType, columnName: String): Boolean =
    lookupValueColumn(schema, columnName).isDefined

  /**
    * Take value that refers to a Column and creates an Expression referring to it.
    * @param columnReference - String that is prefixes with backtick that represents a column name.
    * @return UnresolvedAttribute
    */
  def getValueColumn(columnReference: String): Expression = UnresolvedAttribute(columnReference.stripPrefix(backtick))

  /**
    * Takes Json Number and creates a Expression representing it.
    * @param dataType - Type of column we are comparing to.
    * @param json - Value.
    * @return Expression - Literal representing Json Value as dataType.
    */
  def createNumericLiteral(dataType: DataType, json: JsonNumber): Expression = dataType match {
    case ByteType => Literal.create(json.toByte.get, dataType)
    case ShortType => Literal.create(json.toShort.get, dataType)
    case IntegerType => Literal.create(json.toInt.get, dataType)
    case LongType => Literal.create(json.toLong.get, dataType)
    case DoubleType => Literal.create(json.toDouble, dataType)
    case _ =>
      logger.error(s"Unexpected type: $dataType JsonNum: $json")
      Literal.create(json.toString, dataType)
  }

  /**
    * Creates an expression based on the json. If its a string prefixed with backtick, treat is as a column.
    * @param dataType - Type of column we are comparing to
    * @param json - can be a reference to column, numeric, or string.
    * @return Expression
    */
  def createLiteralOrUnresolvedAttribute(dataType: DataType, json: Json): Expression = {
    if (isValueColumn(json)) {
      val col = json.asString.get
      getValueColumn(col)
    } else {
      createLiteral(dataType, json)
    }
  }

  def createLiteral(dataType: DataType, json: Json): Expression = dataType match {
    case BooleanType => Literal.create(json.asBoolean.get, dataType)
    case x: DataType if x.isInstanceOf[NumericType] => createNumericLiteral(dataType, json.asNumber.get)
    case StringType => Literal.create(json.asString.get, dataType)
    case ut =>
      logger.error(s"type: $ut is not implemented, please report this as bug!")
      Literal.create(json.asString.get)
  }

  def isColumnInDataFrame(dataFrame: DataFrame, column: String): Boolean =
    dataFrame.schema.fields.exists(_.name == column)

  // scalastyle:off cyclomatic.complexity
  def areNumberTypesCompatible(dataType: DataType, number: Option[JsonNumber]): Boolean = {
    val ret = (dataType, number) match {
      case (_, None) => None
      case (ByteType, Some(num)) => num.toByte
      case (ShortType, Some(num)) => num.toShort
      case (IntegerType, Some(num)) => num.toInt
      case (LongType, Some(num)) => num.toLong
      case (DoubleType, num) => num
      case (StringType, num) => num
      case (_, _) => logger.warn(s"Unexpected type: $dataType JsonNum: $number"); None
    }
    logger.debug(s"dataType: $dataType JsonNum: $number")
    ret.isDefined
  }
  // scalastyle:on cyclomatic.complexity

  def areTypesCompatible(dataType: DataType, value: Json): Boolean = {
    val ret = dataType match {
      case BooleanType => value.asBoolean.isDefined
      case x: DataType if x.isInstanceOf[NumericType] => areNumberTypesCompatible(dataType, value.asNumber)
      case StringType => true
      case ut => logger.error(s"type: $ut is not implemented, please report this as bug!")
        false // Fail Unimplemented.
    }
    logger.debug(s"areTypesCompatible colType: $dataType value[${value.getClass.getCanonicalName}]: $value ret: $ret")
    ret
  }
}
