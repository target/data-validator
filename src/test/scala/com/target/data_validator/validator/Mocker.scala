package com.target.data_validator.validator

import com.target.data_validator._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import io.circe.Json

trait Mocker{

  def mkDataFrame(spark: SparkSession, data: List[Row], schema: StructType): DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  def mkPrams(stringParams: List[Tuple2[String, String]] = List.empty, jsonParams: List[Tuple2[String, Json]] = List.empty): VarSubstitution = {
    val dict = new VarSubstitution
    stringParams.foreach(pair => dict.addString(pair._1, pair._2))
    jsonParams.foreach(pair => dict.add(pair._1, pair._2))
    dict
  }
}
