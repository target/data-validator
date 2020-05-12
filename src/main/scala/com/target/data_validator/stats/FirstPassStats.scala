package com.target.data_validator.stats

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

case class FirstPassStats(count: Long, mean: Double, min: Double, max: Double)

object FirstPassStats {
  def dataType: DataType = ScalaReflection
    .schemaFor[FirstPassStats]
    .dataType
}
