package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import com.target.data_validator.TestHelpers._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class NullCheckSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  describe("NullCheck") {
    describe("config parsing") {
      it("basic config") {
        val json = parseYaml(
          """
            |type: nullCheck
            |column: foo
          """.stripMargin)

        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(NullCheck("foo", None)))
      }

      it("optional threshold") {
        val json = parseYaml(
          """
            |type: nullCheck
            |column: foo
            |threshold: 10.0%
          """.stripMargin)

        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(NullCheck("foo", Some("10.0%"))))
      }
      it("config error") {
        val json = parseYaml(
          """
            |type: nullCheck
            |garbage
          """.stripMargin)

        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft) // Todo: Maybe add better check. Left is generic failure.
      }

    }

    describe("variable substitution") {
      it("success substitution") {
        val dict = mkDict("threshold" -> "20%", "column" -> "foo")
        val sut = NullCheck("$column", Some("$threshold"))
        assert(sut.substituteVariables(dict) == NullCheck("foo", Some("20%")))
        assert(!sut.failed)
      }

      it("error on substitution issues") {
        val dict = mkDict()
        val sut = NullCheck("$column", Some("$threshold"))
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

    describe("checkconfiguration") {

      it("Column Exists") {
        val df = mkDf(spark = spark, "item" -> List("Eggs"))
        val sut = NullCheck("item", None)
        assert(!sut.configCheck(df))
      }

      it("Column doesn't exist") {
        val df = mkDf(spark = spark, "item" -> List("Eggs"),
          "price" -> List(0.99), "perishable" -> List(true))
        val sut = NullCheck("junk", None)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: junk not found in schema."))
      }
    }

    describe("functionality") {

      it("success") {
        val df = mkDf(spark, "item" -> List("item1", "item2", "item3"))
        val sut = ValidatorDataFrame(df, None, None, List(NullCheck("item", None)))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("fails") {
        val df = mkDf(spark, "item"-> List("item1", "item2", "item3", null)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NullCheck("item", None)))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)

      }

      it("threshold success") {
        val df = mkDf(spark, "item"-> List("item1", "item2", "item3", null)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NullCheck("item", Some("1"))))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)

      }

      it("threshold failure") {
        val df = mkDf(spark, "item"-> List("item1", "item2", "item3", null, null)) // scalastyle:ignore
        val sut = ValidatorDataFrame(df, None, None, List(NullCheck("item", Some("1"))))
        val config = ValidatorConfig(1, 1, None, false, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }
    }
  }
}
