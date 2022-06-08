package com.target.data_validator

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object GenTestData {

  val schema = StructType(
    List(
      StructField("id", IntegerType),
      StructField("label", StringType),
      StructField("div7", StringType, nullable = true)
    )
  )

  val label: Vector[String] = Vector("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")

  def mkLabel(x: Int): List[String] = {
    if (x == 0) {
      Nil
    } else {
      val y = x % 10
      label(y) :: mkLabel(x / 10)
    }
  }

  def genData(spark: SparkSession): DataFrame = {
    val rg = spark.sparkContext.parallelize(Range(0, 100)) // scalastyle:off magic.number
    spark.createDataFrame(
      rg.map(x => Row(x, mkLabel(x).reverse.mkString(" "), if (x % 7 == 0) null else "NotNull") // scalastyle:off null
      ),
      schema
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("genTestData")
      .master(args.headOption.getOrElse("local"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // Spark is very noisy.

    try {
      val df = genData(spark).coalesce(1)
      df.write.orc("testData.orc")
    } finally {
      spark.stop()
    }
  }

}
