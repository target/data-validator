package com.target.data_validator.stats

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

case class FirstPassStats(count: Long, mean: Double, min: Double, max: Double)

object FirstPassStats {
  def dataType: DataType = ScalaReflection
    .schemaFor[FirstPassStats]
    .dataType

  /**
    * Convert from Spark SQL row format to case class [[FirstPassStats]] format.
    *
    * @param row a complex column of [[org.apache.spark.sql.types.StructType]] output of [[FirstPassStatsAggregator]]
    * @return struct format converted to [[FirstPassStats]]
    */
  def fromRowRepr(row: Row): FirstPassStats = {
    FirstPassStats(
      count = row.getLong(0),
      mean = row.getDouble(1),
      min = row.getDouble(2),
      max = row.getDouble(3)
    )
  }

}
