package com.target.data_validator.stats

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Calculate the count, mean, min and maximum values of a numeric column.
  */
class FirstPassStatsAggregator extends UserDefinedAggregateFunction {

  /**
    * input is a single column of `DoubleType`
    */
  override def inputSchema: StructType = new StructType().add("value", DoubleType)

  /**
    * buffer keeps state for the count, sum, min and max
    */
  override def bufferSchema: StructType = new StructType()
    .add(StructField("count", LongType))
    .add(StructField("sum", DoubleType))
    .add(StructField("min", DoubleType))
    .add(StructField("max", DoubleType))

  private val count = bufferSchema.fieldIndex("count")
  private val sum = bufferSchema.fieldIndex("sum")
  private val min = bufferSchema.fieldIndex("min")
  private val max = bufferSchema.fieldIndex("max")

  /**
    * specifies the return type when using the UDAF
    */
  override def dataType: DataType = FirstPassStats.dataType

  /**
    * These calculations are deterministic
    */
  override def deterministic: Boolean = true

  /**
    * set the initial values for count, sum, min and max
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(count) = 0L
    buffer(sum) = 0.0
    buffer(min) = Double.MaxValue
    buffer(max) = Double.MinValue
  }

  /**
    * update the count, sum, min and max buffer values
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(count) = buffer.getLong(count) + 1
    buffer(sum) = buffer.getDouble(sum) + input.getDouble(0)
    buffer(min) = math.min(input.getDouble(0), buffer.getDouble(min))
    buffer(max) = math.max(input.getDouble(0), buffer.getDouble(max))
  }

  /**
    * reduce the count, sum, min and max values of two buffers
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(count) = buffer1.getLong(count) + buffer2.getLong(count)
    buffer1(sum) = buffer1.getDouble(sum) + buffer2.getDouble(sum)
    buffer1(min) = math.min(buffer1.getDouble(min), buffer2.getDouble(min))
    buffer1(max) = math.max(buffer1.getDouble(max), buffer2.getDouble(max))
  }

  /**
    * evaluate the count, mean, min and max values of a column
    */
  override def evaluate(buffer: Row): Any = {
    FirstPassStats(
      buffer.getLong(count),
      buffer.getDouble(sum) / buffer.getLong(count),
      buffer.getDouble(min),
      buffer.getDouble(max)
    )
  }

}