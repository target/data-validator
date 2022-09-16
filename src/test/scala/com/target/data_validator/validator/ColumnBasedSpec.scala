package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.Json
import io.circe.parser._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnBasedSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  describe("columnMaxCheck") {

    val schema = StructType(
      List(
        StructField("key", StringType),
        StructField("data", StringType),
        StructField("number", IntegerType),
        StructField("byte", ByteType),
        StructField("double", DoubleType)
      )
    )

    val sampleData = List(
      Row("one", "2018/10/01", 3, 10.toByte, 2.0),
      Row("two", "2018/10/02", 2, 20.toByte, 3.5),
      Row("three", "2018/10/31", 1, 30.toByte, 1.7)
    )

    def mkValidatorConfig(checks: List[ValidatorBase]): ValidatorConfig =
      ValidatorConfig(
        1,
        10, // scalastyle:ignore magic.number
        None,
        detailedErrors = false,
        None,
        None,
        List(ValidatorDataFrame(spark.createDataFrame(sc.parallelize(sampleData), schema), None, None, checks))
      )

    it("should be able to be configured from json/YAML") {
      val json = """{ "type": "columnMaxCheck", "column": "rel_d", "value": "2018/10/20" }"""
      assert(
        decode[ValidatorBase](json)(JsonDecoders.decodeChecks) ==
          Right(ColumnMaxCheck("rel_d", Json.fromString("2018/10/20")))
      )
    }

    it("should fail when column doesn't exist") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("junk", Json.fromString("2018/10/31"))))
      assert(sut.configCheck(spark, dict))
      assert(sut.failed)
    }

    it("should not fail when value matches max column value") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("data", Json.fromString("2018/10/31"))))
      assert(!sut.configCheck(spark, dict))
      assert(!sut.quickChecks(spark, dict))
      assert(!sut.failed)
    }

    it("should fail when value doesn't match max column value") {
      val dict = new VarSubstitution
      val columnMaxCheck = ColumnMaxCheck("data", Json.fromString("2018/11/01"))
      val sut = mkValidatorConfig(List(columnMaxCheck))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(
        columnMaxCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap("expected" -> "2018/11/01", "actual" -> "2018/10/31"),
          "ColumnMaxCheck data[StringType]: Expected: 2018/11/01 Actual: 2018/10/31"
        )
      )
    }

    it("should not fail with numeric column matches max value") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("number", Json.fromInt(3))))
      assert(!sut.configCheck(spark, dict))
      assert(!sut.quickChecks(spark, dict))
      assert(!sut.failed)
    }

    it("should fail when numeric column doesn't match max value") {
      val dict = new VarSubstitution
      val columnMaxCheck = ColumnMaxCheck("number", Json.fromInt(100)) // scalastyle:ignore magic.number
      val sut = mkValidatorConfig(List(columnMaxCheck))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(
        columnMaxCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap("expected" -> "100", "actual" -> "3", "relative_error" -> "97.00%"),
          "ColumnMaxCheck number[IntegerType]: Expected: 100 Actual: 3 Relative Error: 97.00%"
        )
      )
    }

    it("should fail with undefined error % when numeric column doesn't match max value and expected value is 0") {
      val dict = new VarSubstitution
      val columnMaxCheck = ColumnMaxCheck("number", Json.fromInt(0))
      val sut = mkValidatorConfig(List(columnMaxCheck))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(
        columnMaxCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap("expected" -> "0", "actual" -> "3", "relative_error" -> "undefined"),
          "ColumnMaxCheck number[IntegerType]: Expected: 0 Actual: 3 Relative Error: undefined"
        )
      )
    }

    it("should not fail when double column matches max value") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("double", Json.fromDouble(3.5).get)))
      assert(!sut.configCheck(spark, dict))
      assert(!sut.quickChecks(spark, dict))
      assert(!sut.failed)
    }

    it("should fail when double column doesn't match max value") {
      val dict = new VarSubstitution
      val columnMaxCheck = ColumnMaxCheck("double", Json.fromDouble(5.0).get)
      val sut = mkValidatorConfig(List(columnMaxCheck))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(
        columnMaxCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap("expected" -> "5.0", "actual" -> "3.5", "relative_error" -> "30.00%"),
          "ColumnMaxCheck double[DoubleType]: Expected: 5.0 Actual: 3.5 Relative Error: 30.00%"
        )
      )
    }

    it("should fail when byte column and value overflows") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("byte", Json.fromInt(1000)))) // scalastyle:ignore
      assert(sut.configCheck(spark, dict))
      assert(sut.failed)
    }

    it("should fail when byte column and string value") {
      val dict = new VarSubstitution
      val sut = mkValidatorConfig(List(ColumnMaxCheck("byte", Json.fromString("bit"))))
      assert(sut.configCheck(spark, dict))
      assert(sut.failed)
    }

    it("variable substitution should produce VarSubJsonEvent()") {
      val vars = new VarSubstitution
      vars.addString("col", "byte")
      val sut = ColumnMaxCheck("${col}", Json.fromInt(100)).substituteVariables(vars) // scalastyle:ignore
      assert(!sut.failed)
      assert(sut.getEvents contains VarSubEvent("${col}", "byte"))
    }

  }

}
