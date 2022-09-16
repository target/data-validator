package com.target.data_validator.validator

import com.target.data_validator._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import io.circe.Json

trait Mocker {

  def mkDataFrame(spark: SparkSession, data: List[Row], schema: StructType): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

  def mkParams(params: List[Tuple2[String, Any]] = List.empty): VarSubstitution = {
    val dict = new VarSubstitution
    params.foreach { pair =>
      pair._2 match {
        case p: Json => dict.add(pair._1, pair._2.asInstanceOf[Json])
        case p: String => dict.addString(pair._1, pair._2.asInstanceOf[String])
      }
    }
    dict
  }
}
