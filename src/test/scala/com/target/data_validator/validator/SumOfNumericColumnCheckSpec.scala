package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import com.target.data_validator.validator.SumOfNumericColumnCheck._
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest.{FunSpec, Matchers}

class SumOfNumericColumnCheckSpec
  extends FunSpec
    with Matchers
    with TestingSparkSession
    with SumOfNumericColumnCheckExamples
    with SumOfNumericColumnCheckBasicSetup {

  describe("SumOfNumericColumnCheck") {
    describe("config parsing") {
      it("uses over threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: over
             |threshold: ${expectedThreshold._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "over", threshold = Some(expectedThreshold._2))))
      }
      it("uses under threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: under
             |threshold: ${expectedThreshold._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "under", threshold = Some(expectedThreshold._2))))
      }
      it("uses between threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: between
             |lowerBound: ${expectedLower._1}
             |upperBound: ${expectedUpper._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "between",
            lowerBound = Some(expectedLower._2), upperBound = Some(expectedUpper._2))))
      }
      it("uses outside threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: outside
             |lowerBound: ${expectedLower._1}
             |upperBound: ${expectedUpper._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "outside",
            lowerBound = Some(expectedLower._2), upperBound = Some(expectedUpper._2))))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |thresholdType: over
             |threshold: ${expectedThreshold}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }
      it("is missing the threshold entirely") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }
    }

    describe("variable substitution") {
      it("success substitution") {
        var dict = mkDict("threshold" -> "20", "column" -> "foo", "thresholdType" -> "under")
        var sut = SumOfNumericColumnCheck("$column", "$thresholdType", Some(Json.fromString("$threshold")))
        assert(sut.substituteVariables(dict) ==
          SumOfNumericColumnCheck("foo", "under", Some(Json.fromInt(20))))
        assert(!sut.failed)
      }

      it("error on substitution issues") {
        var dict = mkDict()
        var sut = SumOfNumericColumnCheck("$column", "$thresholdType", Some(Json.fromString("$threshold")))
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "column"))
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "threshold"))
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "thresholdType"))
      }
    }

    describe("check configuration") {
      it("Column Exists") {
        val df = mkDf(spark = spark, "price" -> List(1.99))
        val sut = overCheck
        assert(!sut.configCheck(df))
      }

      it("Column doesn't exist") {
        val df = mkDf(spark = spark, ("lolnope" -> List(1.99)))
        val sut = overCheck
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price not found in schema."))
      }

      it("Column exists but is wrong type") {
        val df = mkDf(spark = spark, ("price" -> List("eggs")))
        val sut = overCheck
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price found, but not of numericType type: StringType"))
      }
    }

    describe("functionality") {
      it("success") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = ValidatorDataFrame(
          df = df,
          keyColumns = None,
          condition = None,
          checks = List(underCheck))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("fails") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(underCheck))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)

      }

      it("threshold success") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(underCheck))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)

      }

      it("threshold failure") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(underCheck))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }
    }
  }
}
trait SumOfNumericColumnCheckBasicSetup {
  val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
}
trait SumOfNumericColumnCheckExamples {
  val expectedThreshold = (8, Json.fromInt(8))
  val expectedLower = (2, Json.fromInt(2))
  val expectedUpper = (10, Json.fromInt(10))

  val listWithSum6 = "price" -> List(1, 2, 3)

  val overCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "over", threshold = Some(expectedThreshold._2))
  val underCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "under", threshold = Some(expectedThreshold._2))
  val betweenCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "between",
    lowerBound = Some(expectedLower._2), upperBound = Some(expectedUpper._2))
  val outsideCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "outside",
    lowerBound = Some(expectedLower._2), upperBound = Some(expectedUpper._2))
}
