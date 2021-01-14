package com.target.data_validator

import com.typesafe.scalalogging.LazyLogging
import io.circe.{parser, Json}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object JsonUtils extends LazyLogging {

  def string2Json(v: String): Json = parser.parse(v) match {
    case Right(b) => b
    case Left(_) => Json.fromString(v)
  }

  // scalastyle:off cyclomatic.complexity
  def debugJson(j: Json): String = j match {
    case _ if j.isNull => s"Json NULL"
    case _ if j.isNumber => s"Json NUM: ${j.asNumber.get}"
    case _ if j.isArray => s"Json ARR: ${j.noSpaces}"
    case _ if j.isBoolean => s"Json BOOLEAN: ${j.asBoolean.get}"
    case _ if j.isObject => s"Json OBJECT: ${j.noSpaces}"
    case _ if j.asString.isDefined => s"Json STRING: ${j.asString.get}"
    case _ => s"Json UNKNOWN[${j.getClass.getSimpleName}]: ${j.noSpaces}"
  }

  /**
    * Turn Row into JSon
    * @return Json Object
    */
  def row2Json(row: Row): Json = {
    val fields = row.schema.fieldNames.zipWithIndex.map {
      case (fieldName, idx) => (fieldName, row2Json(row, idx))
    }
    Json.obj(fields: _*)
  }

  /**
    * Take Row, and turn col into Json.
    * @return Json
    */
  def row2Json(row: Row, col: Int): Json = {
    val dataType = row.schema(col).dataType
    dataType match {
      case StringType => Json.fromString(row.getString(col))
      case LongType => Json.fromLong(row.getLong(col))
      case IntegerType => Json.fromInt(row.getInt(col))
      case NullType => Json.Null
      case BooleanType => Json.fromBoolean(row.getBoolean(col))
      case DoubleType => Json.fromDoubleOrNull(row.getDouble(col))
      case _: StructType => row2Json(row.getStruct(col))
      case _ =>
        logger.error(s"Unimplemented dataType '${dataType.typeName}' in column: ${row.schema(col).name} " +
          "Please report this as a bug.")
        Json.Null
    }
  }
  // scalastyle:on cyclomatic.complexity
}
