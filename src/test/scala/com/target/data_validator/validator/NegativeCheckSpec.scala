package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import org.scalatest.{FunSpec, Matchers}

class NegativeCheckSpec extends FunSpec with Matchers with TestingSparkSession {

  describe("NullCheck") {

      describe("config parsing") {
        it("basic config") {
          val json = parseYaml(
            """
              |type: negativeCheck
              |column: foo
            """.stripMargin)

          val sut = JsonDecoders.decodeChecks.decodeJson(json)
          assert(sut == Right(NegativeCheck("foo", None)))
        }

        it("optional threshold") {
          val json = parseYaml(
            """
              |type: negativeCheck
              |column: foo
              |threshold: 10.0%
            """.stripMargin)

          val sut = JsonDecoders.decodeChecks.decodeJson(json)
          assert(sut == Right(NegativeCheck("foo", Some("10.0%"))))
        }
        it("config error") {
          val json = parseYaml(
            """
              |type: negativeCheck
              |garbage
            """.stripMargin)

          val sut = JsonDecoders.decodeChecks.decodeJson(json)
          assert(sut.isLeft) // Todo: Maybe add better check. Left is generic failure.
        }

      }

    describe("variable substitution") {
      it("success substitution") {
        var dict = mkDict("threshold" -> "20%", "column" -> "foo")
        var sut = NegativeCheck("$column", Some("$threshold"))
        assert(sut.substituteVariables(dict) == NegativeCheck("foo", Some("20%")))
        assert(!sut.failed)
      }

      it("error on substitution issues") {
        var dict = mkDict()
        var sut = NegativeCheck("$column", Some("$threshold"))
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "column"))
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "threshold"))
      }
    }

    describe("check configuration") {
      it("Column Exists") {
        val df = mkDf(spark = spark, "price" -> List(1.99))
        val sut = NegativeCheck("price", None)
        assert(!sut.configCheck(df))
      }

      it("Column doesn't exist") {
        val df = mkDf(spark = spark, ("price" -> List(1.99)))
        val sut = NegativeCheck("junk", None)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: junk not found in schema."))
      }

      it("Column exists but is wrong type") {
        val df = mkDf(spark = spark, ("item" -> List("eggs")))
        val sut = NegativeCheck("item", None)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: item found, but not of numericType type: StringType"))
      }

    }

    describe("functionality") {

      it("success") {
        val df = mkDf(spark, "price"-> List(1.99, 1.50, 2.50)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NegativeCheck("price", None)))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("fails") {
        val df = mkDf(spark, "price"-> List(1.99, -1.50, 2.50)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NegativeCheck("price", None)))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)

      }

      it("threshold success") {
        val df = mkDf(spark, "price"-> List(1.99, -1.50, 2.50)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NegativeCheck("price", Some("1"))))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)

      }

      it("threshold failure") {
        val df = mkDf(spark, "price"-> List(1.99, -1.50, -2.50)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NegativeCheck("price", Some("1"))))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }
    }
  }
}
