package com.target.data_validator.stats

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.DataType

case class SecondPassStats(stdDev: Double, histogram: Histogram)

object SecondPassStats {
  def dataType: DataType = ScalaReflection
    .schemaFor[SecondPassStats]
    .dataType

  /** Convert from Spark SQL row format to case class [[SecondPassStats]] format.
    *
    * @param row
    *   a complex column of [[org.apache.spark.sql.types.StructType]] output of [[SecondPassStatsAggregator]]
    * @return
    *   struct format converted to [[SecondPassStats]]
    */
  def fromRowRepr(row: Row): SecondPassStats = {
    SecondPassStats(
      stdDev = row.getDouble(0),
      histogram = Histogram(
        row.getStruct(1).getSeq[Row](0) map { bin =>
          Bin(
            lowerBound = bin.getDouble(0),
            upperBound = bin.getDouble(1),
            count = bin.getLong(2)
          )
        }
      )
    )
  }

}
