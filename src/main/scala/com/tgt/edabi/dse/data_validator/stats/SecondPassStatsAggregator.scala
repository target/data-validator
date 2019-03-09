package com.tgt.edabi.dse.data_validator.stats

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Calculate the standard deviation and histogram of a numeric column
  */
class SecondPassStatsAggregator(firstPassStats: FirstPassStats) extends UserDefinedAggregateFunction {

  val NUMBER_OF_BINS = 10

  private val binSize = (firstPassStats.max - firstPassStats.min) / NUMBER_OF_BINS
  private val upperBounds = for (i <- 1 to NUMBER_OF_BINS) yield { firstPassStats.min + i * binSize }

  /**
    * input is a single column of `DoubleType`
    */
  override def inputSchema: StructType = new StructType().add("value", DoubleType)

  /**
    * buffer keeps state for the total count, sumOfSquares, and individual bin counts
    */
  override def bufferSchema: StructType = StructType(
    List(
      StructField("count",        LongType),
      StructField("sumOfSquares", DoubleType),
      StructField("bin1count",    LongType),
      StructField("bin2count",    LongType),
      StructField("bin3count",    LongType),
      StructField("bin4count",    LongType),
      StructField("bin5count",    LongType),
      StructField("bin6count",    LongType),
      StructField("bin7count",    LongType),
      StructField("bin8count",    LongType),
      StructField("bin9count",    LongType),
      StructField("bin10count",   LongType)
    )
  )

  private val count = bufferSchema.fieldIndex("count")
  private val sumOfSquares = bufferSchema.fieldIndex("sumOfSquares")
  private val binStart = bufferSchema.fieldIndex("bin1count")
  private val binEnd = bufferSchema.fieldIndex("bin10count")

  /**
    * specifies the return type when using the UDAF
    */
  override def dataType: DataType = SecondPassStats.dataType

  /**
    * these calculations are deterministic
    */
  override def deterministic: Boolean = true

  /**
    * set the initial values for count, sum of squares and individual bin counts
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(count) = 0L
    buffer(sumOfSquares) = 0.0
    for (i <- binStart to binEnd) { buffer(i) = 0L }
  }

  /**
    * update the count, sum of squares and individual bin counts
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(count) = buffer.getLong(count) + 1
    buffer(sumOfSquares) = buffer.getDouble(sumOfSquares) + math.pow(input.getDouble(0) - firstPassStats.mean, 2)
    // determine the index of the bin that we should increment
    val binIndex = binStart + math.min(NUMBER_OF_BINS - 1, math.floor((input.getDouble(0) - firstPassStats.min) / binSize).toInt)
    buffer(binIndex) = buffer.getLong(binIndex) + 1
  }

  /**
    * reduce the count, sum of squares and individual bin counts
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(count) = buffer1.getLong(count) + buffer2.getLong(count)
    buffer1(sumOfSquares) = buffer1.getDouble(sumOfSquares) + buffer2.getDouble(sumOfSquares)
    for (i <- binStart to binEnd) {
      buffer1(i) = buffer1.getLong(i) + buffer2.getLong(i)
    }
  }

  /**
    * evaluate the standard deviation and define bins of histogram
    */
  override def evaluate(buffer: Row): Any = {
    val bins: Seq[Bin] = for (i <- binStart to binEnd) yield {
      val bIndex = i - binStart
      i match {
        case start if i == binStart => Bin(firstPassStats.min, upperBounds(bIndex), buffer.getLong(start))
        case end if i == binEnd => Bin(upperBounds(bIndex - 1), firstPassStats.max, buffer.getLong(end))
        case _ => Bin(upperBounds(bIndex - 1), upperBounds(bIndex), buffer.getLong(i))
      }
    }
    SecondPassStats(
      math.sqrt(buffer.getDouble(sumOfSquares) / (buffer.getLong(count) - 1)),
      Histogram(bins)
    )
  }

}