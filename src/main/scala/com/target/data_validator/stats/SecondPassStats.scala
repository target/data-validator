package com.target.data_validator.stats

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

case class SecondPassStats(stdDev: Double, histogram: Histogram)

object SecondPassStats {
  def dataType: DataType = ScalaReflection
    .schemaFor[SecondPassStats]
    .dataType
}
