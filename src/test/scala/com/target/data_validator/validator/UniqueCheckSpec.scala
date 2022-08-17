package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class UniqueCheckSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  val schema = StructType(List(StructField("item", StringType),
    StructField("location", IntegerType),
    StructField("price", DoubleType)))

  val defData = List(Row("Eggs", 1, 4.00), Row("Milk", 1, 10.27),
    Row("Eggs", 1, 5.00), Row("Eggs", 2, 2.00))
  def mkDataFrame(spark: SparkSession, data: List[Row]): DataFrame = spark.createDataFrame(sc.parallelize(data), schema)

  describe("fromJson") {
    it("create fromJson") {
      import com.target.data_validator.validator.JsonDecoders.decodeChecks
      val yaml =
        """---
          |- type: uniqueCheck
          |  columns:
          |   - foo
          |   - bar
        """.stripMargin
      val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
      val sut = json.as[Array[ValidatorBase]]
      assert(sut.isRight)
      assert(sut.right.get contains UniqueCheck(Array("foo", "bar")))
    }
  }

  describe ("substituteVariables") {
    it("replaces variables") {
      val dict = new VarSubstitution
      dict.addString("col1", "foo")
      dict.addString("col2", "bar")
      val sut = UniqueCheck(List("${col1}", "$col2"))
      assert(sut.substituteVariables(dict) == UniqueCheck(List("foo", "bar")))
      assert(!sut.failed)
    }

  }

  describe ("configCheck") {
    it("good columns") {
      val sut = UniqueCheck(List("item", "location"))
      val df = mkDataFrame(spark, defData)
      assert(!sut.configCheck(df))
      assert(!sut.failed)
    }

    it("bad column") {
      val sut = UniqueCheck(List("item", "city"))
      val df = mkDataFrame(spark, defData)
      assert(sut.configCheck(df))
      assert(sut.failed)
    }

  }

  describe("costlyCheck") {

    it("finds error") {
      val sut = UniqueCheck(Seq("item"))
      val df = mkDataFrame(spark, defData)
      assert(sut.costlyCheck(df))
      assert(sut.failed)
      assert(sut.getEvents contains ValidatorError("1 duplicates found!"))
    }

    it("finds error with multiple columns") {
      val sut = UniqueCheck(Seq("item", "location"))
      val df = mkDataFrame(spark, defData)
      assert(sut.costlyCheck(df))
      assert(sut.failed)
      assert(sut.getEvents contains ValidatorError("1 duplicates found!"))
    }

    it("no error") {
      val sut = UniqueCheck(Seq("price"))
      val df = mkDataFrame(spark, defData)
      assert(!sut.costlyCheck(df))
      assert(!sut.failed)
      assert(sut.getEvents contains ValidatorGood("no duplicates found."))
    }
  }

  describe("toJson") {

    it("generates correct json") {
      val sut = UniqueCheck(Seq("item"))
      assert(sut.toJson == Json.fromFields(Seq(
        ("type", Json.fromString("uniqueCheck")),
        ("columns", Json.fromValues(List(Json.fromString("item")))),
        ("failed", Json.fromBoolean(false)),
        ("events", Json.fromValues(Seq.empty)))))
    }
  }

  describe("completeExample") {
    it("happy path that finds error") {
      val uc = UniqueCheck(List("item"))
      val dict = new VarSubstitution
      val df = mkDataFrame(spark, defData)
      val sut = ValidatorConfig(1, 1, None, detailedErrors = false, None, None,
        List(ValidatorDataFrame(df, None, None, List(uc))))

      assert(!sut.configCheck(spark, dict))
      assert(!sut.quickChecks(spark, dict))
      assert(sut.costlyChecks(spark, dict))
      assert(sut.failed)
    }

    it("happy path that doesn't find error") {
      val uc = UniqueCheck(List("price"))
      val dict = new VarSubstitution
      val df = mkDataFrame(spark, defData)
      val sut = ValidatorConfig(1, 1, None, detailedErrors = false, None, None,
        List(ValidatorDataFrame(df, None, None, List(uc))))

      assert(!sut.configCheck(spark, dict))
      assert(!sut.costlyChecks(spark, dict))
      assert(!sut.failed)
    }
  }

}
