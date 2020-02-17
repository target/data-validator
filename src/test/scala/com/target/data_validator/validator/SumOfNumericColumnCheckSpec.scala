package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import com.target.data_validator.validator.SumOfNumericColumnCheck._
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import org.scalatest.{FunSpec, Matchers}

class SumOfNumericColumnCheckSpec extends FunSpec with Matchers with TestingSparkSession {

  val expectedThreshold = 4
  val expectedLower = 2
  val expectedUpper = 10

  describe("SumOfNumericColumnCheck") {
    describe("config parsing") {
      it("uses over threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |threshold:
             |  type: over
             |  threshold: ${expectedThreshold}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(SumOfNumericColumnCheck("foo", Over(expectedThreshold))))
      }
      it("uses under threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |threshold:
             |  type: under
             |  threshold: ${expectedThreshold}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(SumOfNumericColumnCheck("foo", Under(expectedThreshold))))
      }
      it("uses between threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |threshold:
             |  type: between
             |  lower: ${expectedLower}
             |  upper: ${expectedUpper}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(SumOfNumericColumnCheck("foo", Between(expectedLower, expectedUpper))))
      }
      it("uses outside threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |threshold:
             |  type: outside
             |  lower: ${expectedLower}
             |  upper: ${expectedUpper}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(SumOfNumericColumnCheck("foo", Outside(expectedLower, expectedUpper))))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |threshold:
             |  type: over
             |  threshold: ${expectedThreshold}
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
      it("is tries to use an invalid threshold type") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |threshold:
             |  type: overrrrrrrrrrr
             |  threshold: ${expectedThreshold}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }
    }
  }

}
