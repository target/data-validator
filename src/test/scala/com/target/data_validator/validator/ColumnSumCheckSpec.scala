package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ColumnBasedValidatorCheckEvent, ValidatorConfig, ValidatorDataFrame, ValidatorError}
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import io.circe._
import org.scalatest.{FunSpec, Matchers}

import scala.collection.immutable.ListMap

class ColumnSumCheckSpec extends FunSpec with Matchers with TestingSparkSession {

  describe("ColumnSumCheck") {

    val columnSumCheck = ColumnSumCheck(
      column = "foo",
      minValue = Some(Json.fromInt(2)),
      maxValue = Some(Json.fromDouble(3.14).get),
      inclusive = Some(Json.fromBoolean(true))
    )

    describe("toJson") {

      it("lower bound is optional") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |maxValue: 3.14
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(columnSumCheck.copy(minValue = None)))
      }

      it("upper bound is optional") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: 2
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(columnSumCheck.copy(maxValue = None)))
      }

      it("inclusive is optional") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: 2
             |maxValue: 3.14
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(columnSumCheck.copy(inclusive = None)))
      }

      it("column is required") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |minValue: 2
             |maxValue: 3.14
             |inclusive: false
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }

    }

    describe("substituteVariables") {

      it("'column' is substitutable") {
        val dict = mkDict("column" -> "bar")
        val sut = columnSumCheck.copy(column = "$column")
        assert(sut.substituteVariables(dict) == columnSumCheck.copy(column = "bar"))
        assert(!sut.failed)
      }

      it("'minValue' is substitutable") {
        val dict = mkDict("minValue" -> "1")
        val sut = columnSumCheck.copy(minValue = Some(Json.fromString("$minValue")))
        assert(sut.substituteVariables(dict) == columnSumCheck.copy(minValue = Some(Json.fromInt(1))))
        assert(!sut.failed)
      }

      it("'maxValue' is substitutable") {
        val dict = mkDict("maxValue" -> "4.1")
        val sut = columnSumCheck.copy(maxValue = Some(Json.fromString("$maxValue")))
        assert(sut.substituteVariables(dict) == columnSumCheck.copy(maxValue = Some(Json.fromDouble(4.1).get)))
        assert(!sut.failed)
      }

      it("'inclusive' is substitutable") {
        val dict = mkDict("inclusive" -> "false")
        val sut = columnSumCheck.copy(inclusive = Some(Json.fromString("$inclusive")))
        assert(sut.substituteVariables(dict) == columnSumCheck.copy(inclusive = Some(Json.fromBoolean(false))))
        assert(!sut.failed)
      }

    }

    describe("configCheck") {

      it("valid configuration") {
        val df = mkDf(spark, "foo" -> List(1.99))
        assert(!columnSumCheck.configCheck(df))
      }

      it("'minValue' and 'maxValue' not present") {
        val df = mkDf(spark, "foo" -> List(1.99))
        val sut = columnSumCheck.copy(minValue = None, maxValue = None)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("'minValue' or 'maxValue' or both must be defined"))
      }

      it("'inclusive' is a bad value") {
        val df = mkDf(spark, "foo" -> List(1.99))
        val sut = columnSumCheck.copy(inclusive = Some(Json.fromString("bar")))
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorError("'inclusive' defined but type is not Boolean, is: String")
        )
      }

      it("'minValue' not less than 'maxValue'") {
        val df = mkDf(spark = spark, "foo" -> List(1.99))
        val sut = columnSumCheck.copy(maxValue = Some(Json.fromInt(1)))
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("'minValue': 2.0 must be less than 'maxValue': 1.0"))
      }

      it("'minValue' and 'maxValue' must be numbers") {
        val df = mkDf(spark = spark, "foo" -> List(1.99))
        val sut = columnSumCheck.copy(minValue = Some(Json.fromString("bar")), maxValue = Some(Json.fromString("2")))
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("'minValue' defined but type is not a Number, is: String"))
        assert(sut.getEvents contains ValidatorError("'maxValue' defined but type is not a Number, is: String"))
      }

      it("'column' does not exist") {
        val df = mkDf(spark = spark, "bar" -> List(1.99))
        val sut = columnSumCheck
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: foo not found in schema"))
      }

      it("'column' exists but is wrong type") {
        val df = mkDf(spark = spark, "foo" -> List("bar"))
        val sut = columnSumCheck
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: foo found, but not of numericType type: StringType"))
      }

    }

    describe("quickCheck") {

      val detailedErrors = true
      val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)

      it("lower bound success") {
        val check = ColumnSumCheck("foo", minValue = Some(Json.fromInt(0)))
        val df = mkDf(spark, "foo" -> List(1.0, 2.0, 1.5))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
        // assert(sut.getEvents contains ValidatorCheckEvent(failure = false, "columnSumCheck on 'foo'", 3, 1))
      }

      it("lower bound failure") {
        val check = ColumnSumCheck("foo", minValue = Some(Json.fromInt(6))) // scalastyle:ignore magic.number
        val df = mkDf(spark, "foo" -> List(1, 2, 3))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
        assert(check.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap(
            "lower_bound" -> "6",
            "inclusive" -> "false",
            "actual" -> "6",
            "relative_error" -> "undefined"
          ),
          "columnSumCheck on foo[LongType]: Expected Range: (6, ) Actual: 6 Relative Error: undefined"
        ))
      }

      it("lower bound inclusive success") {
        val check = ColumnSumCheck(
          "foo",
          minValue = Some(Json.fromInt(6)), // scalastyle:ignore magic.number
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, "foo" -> List(1L, 2L, 3L))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper bound success with short") {
        val check = ColumnSumCheck("foo", maxValue = Some(Json.fromDouble(10).get)) // scalastyle:ignore magic.number
        val df = mkDf(spark, "foo" -> List[Short](1, 2, 1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper bound success with byte") {
        val check = ColumnSumCheck("foo", maxValue = Some(Json.fromDouble(10).get)) // scalastyle:ignore magic.number
        val df = mkDf(spark, "foo" -> List[Byte](1, 2, 1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper bound failure") {
        val check = ColumnSumCheck("foo", maxValue = Some(Json.fromFloat(1).get)) // scalastyle:ignore magic.number
        val df = mkDf(spark, "foo" -> List(1L, 1L, 1L))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
        assert(check.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap(
            "upper_bound" -> "1.0",
            "inclusive" -> "false",
            "actual" -> "3",
            "relative_error" -> "200.00%"
          ),
          "columnSumCheck on foo[LongType]: Expected Range: ( , 1.0) Actual: 3 Relative Error: 200.00%"
        ))
      }

      it("upper bound inclusive success") {
        val check = ColumnSumCheck(
          "foo",
          maxValue = Some(Json.fromLong(3)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, "foo" -> List(1.0, 1.0, 1.0))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper and lower bound success") {
        val check = ColumnSumCheck(
          "foo",
          minValue = Some(Json.fromLong(0)),
          maxValue = Some(Json.fromLong(3))
        )
        val df = mkDf(spark, "foo" -> List(1, 1, -1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper and lower bound failure") {
        val check = ColumnSumCheck(
          "foo",
          minValue = Some(Json.fromInt(0)),
          maxValue = Some(Json.fromInt(2))
        )
        val df = mkDf(spark, "foo" -> List(1, 1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
        assert(check.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap(
            "lower_bound" -> "0",
            "upper_bound" -> "2",
            "inclusive" -> "false",
            "actual" -> "2",
            "relative_error" -> "undefined"
          ),
          "columnSumCheck on foo[LongType]: Expected Range: (0, 2) Actual: 2 Relative Error: undefined"
        ))
      }

      it("upper and lower bound inclusive failure") {
        val check = ColumnSumCheck(
          "foo",
          minValue = Some(Json.fromInt(10)), // scalastyle:ignore magic.number
          maxValue = Some(Json.fromInt(20)), // scalastyle:ignore magic.number
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, "foo" -> List(1, 1, 1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
        assert(check.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap(
            "lower_bound" -> "10",
            "upper_bound" -> "20",
            "inclusive" -> "true",
            "actual" -> "3",
            "relative_error" -> "70.00%"
          ),
          "columnSumCheck on foo[LongType]: Expected Range: [10, 20] Actual: 3 Relative Error: 70.00%"
        ))
      }

      it("upper bound and lower inclusive success") {
        val check = ColumnSumCheck(
          "foo",
          minValue = Some(Json.fromInt(0)),
          maxValue = Some(Json.fromInt(2)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, "foo" -> List(1, 1))
        val sut = ValidatorDataFrame(df, None, None, List(check))
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

    }

  }

}
