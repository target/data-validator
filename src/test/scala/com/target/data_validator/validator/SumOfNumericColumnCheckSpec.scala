package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import com.target.data_validator.validator.SumOfNumericColumnCheck._
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest.{FunSpec, Matchers}

class SumOfNumericColumnCheckSpec extends FunSpec with Matchers with TestingSparkSession {

  val expectedThreshold = (4, Json.fromInt(4))
  val expectedLower = (2, Json.fromInt(2))
  val expectedUpper = (10, Json.fromInt(10))

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
      it("successfully substitutes") {
        //var sut = SumOfNumericColumnCheck("$column", InclusiveThreshold(""))
      }
    }
  }

}
