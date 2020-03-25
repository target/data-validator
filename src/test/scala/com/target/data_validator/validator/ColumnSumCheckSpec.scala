package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import io.circe._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, FunSpec, Matchers}

class ColumnSumCheckSpec
  extends FunSpec
    with Matchers
    with TestingSparkSession
    with ColumnSumCheckExamples {

  describe("ColumnSumCheck") {

    describe("config parsing") {

      it("uses lower bound only") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(ColumnSumCheck("foo", minValue = Some(int_8._2))))
      }

      it("uses upper bound only") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |maxValue: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(ColumnSumCheck("foo", maxValue = Some(int_8._2))))
      }

      it("uses lower and upper bound") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_2._1}
             |maxValue: ${int_10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(ColumnSumCheck("foo", minValue = Some(int_2._2), maxValue = Some(int_10._2))))
      }

      it("uses lower bound inclusively") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_8._1}
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          ColumnSumCheck("foo", minValue = Some(int_8._2), inclusive = Some(Json.fromBoolean(true)))
        ))
      }

      it("uses upper bound inclusively") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |maxValue: ${int_8._1}
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          ColumnSumCheck("foo", maxValue = Some(int_8._2), inclusive = Some(Json.fromBoolean(true)))
        ))
      }

      it("uses lower and upper bound inclusively") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_2._1}
             |maxValue: ${int_10._1}
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          ColumnSumCheck(
            "foo",
            minValue = Some(int_2._2),
            maxValue = Some(int_10._2),
            inclusive = Some(Json.fromBoolean(true))
          )
        ))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |maxValue: $int_8
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }

    }

    describe("variable substitution") {

      it("success substitution") {
        val dict = mkDict("minValue" -> "20", "column" -> "foo")
        val sut = ColumnSumCheck("$column", Some(Json.fromString("$minValue")))
        assert(sut.substituteVariables(dict) ==
          ColumnSumCheck("foo", Some(Json.fromInt(20))))
        assert(!sut.failed)
      }

      it("error on substitution issues") {
        val dict = mkDict()
        val sut = ColumnSumCheck("$column", Some(Json.fromString("$minValue")))
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "column"))
      }

    }

    describe("check configuration") {

      it("Column Exists") {
        val df = mkDf(spark = spark, "price" -> List(1.99))
        val sut = overCheckForInt
        assert(!sut.configCheck(df))
      }

      it("Column doesn't exist") {
        val df = mkDf(spark = spark, "lolnope" -> List(1.99))
        val sut = overCheckForInt
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price not found in schema."))
      }

      it("Column exists but is wrong type") {
        val df = mkDf(spark = spark, "price" -> List("eggs"))
        val sut = overCheckForInt
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price found, but not of numericType type: StringType"))
      }

      it("parses and checks without caring about arg order") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |maxValue: ${int_8._1}
             |inclusive: true
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json).right.get
        assert(sut.asInstanceOf[ColumnSumCheck] ==
          ColumnSumCheck("foo", maxValue = Some(int_8._2), inclusive = Some(Json.fromBoolean(true))))
        val df = mkDf(spark = spark, "foo" -> List(6))
        assert(!sut.configCheck(df))
      }
    }

    describe("quickCheck functionality") {

      val detailedErrors = false
      val longListWithSum6 = "price" -> List(1L, 2L, 3L)

      it("no bounds creates an error") {
        val check = ColumnSumCheck("price")
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
        assert(sut.getEvents.exists {
          evt => evt.isInstanceOf[ValidatorError] &&
            evt.asInstanceOf[ValidatorError].msg.contains("min and max tests were None")
        })
      }

      it("lower bound success") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(0L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("lower bound failure") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(6L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }

      it("lower bound inclusive success") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(6L)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper bound success") {
        val check = ColumnSumCheck(
          "price",
          maxValue = Some(Json.fromLong(10L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper bound failure") {
        val check = ColumnSumCheck(
          "price",
          maxValue = Some(Json.fromLong(6L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }

      it("upper bound inclusive success") {
        val check = ColumnSumCheck(
          "price",
          maxValue = Some(Json.fromLong(6L)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper and lower bound success") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(5L)),
          maxValue = Some(Json.fromLong(10L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

      it("upper and lower bound failure") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(1L)),
          maxValue = Some(Json.fromLong(6L))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }

      it("upper bound and lower inclusive success") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(1L)),
          maxValue = Some(Json.fromLong(6L)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }

    }

    describe("json reporting") {
      val detailedErrors = false
      val longListWithSum6 = "price" -> List(1L, 2L, 3L)
      it("emits expected json") {
        val check = ColumnSumCheck(
          "price",
          minValue = Some(Json.fromLong(1L)),
          maxValue = Some(Json.fromLong(6L)),
          inclusive = Some(Json.fromBoolean(true))
        )
        val df = mkDf(spark, longListWithSum6)
        val sut = ValidatorDataFrame(df, None, None, List(check))
        val config = ValidatorConfig(1, 1, None, detailedErrors, None, None, List.empty)
        sut.configCheck(spark, mkDict())
        sut.quickChecks(spark, mkDict())(config)
        val json = sut.checks.head.toJson
        json.asObject.get.keys should contain allOf("type", "failed")
      }
    }
  }
}

trait ColumnSumCheckExamples extends TestPairMakers {
  // Int
  val int_8: (Int, Json) = makeTestPair(8)
  val int_2: (Int, Json) = makeTestPair(2)
  val int_9: (Int, Json) = makeTestPair(9)
  val int_10: (Int, Json) = makeTestPair(10)

  val intListWithSum6: (String, List[Int]) = "price" -> List(1, 2, 3)
  val intListWithSum9: (String, List[Int]) = "price" -> List(3, 3, 3)

  def overCheckForInt: ColumnSumCheck = overCheck(int_8._2)
  def overCheckInclusiveForInt: ColumnSumCheck = overCheckInclusive(int_9._2)
  def underCheckForInt: ColumnSumCheck = underCheck(int_8._2)
  def betweenCheckForInt: ColumnSumCheck = betweenCheck(int_2._2, int_10._2)

  // Long
  val long_8: (Long, Json) = makeTestPair(8L)
  val long_2: (Long, Json) = makeTestPair(2L)
  val long_9: (Long, Json) = makeTestPair(9L)
  val long_10: (Long, Json) = makeTestPair(10L)

  val longListWithSum6: (String, List[Long]) = "price" -> List(1L, 2L, 3L)
  val longListWithSum9: (String, List[Long]) = "price" -> List(3L, 3L, 3L)

  def overCheckForLong: ColumnSumCheck = overCheck(long_8._2)
  def overCheckInclusiveForLong: ColumnSumCheck = overCheckInclusive(long_9._2)
  def underCheckForLong: ColumnSumCheck = underCheck(long_8._2)
  def betweenCheckForLong: ColumnSumCheck = betweenCheck(long_2._2, long_10._2)

  // Double
  val dbl_8: (Double, Json) = makeTestPair(8.0)
  val dbl_2: (Double, Json) = makeTestPair(2.0)
  val dbl_9: (Double, Json) = makeTestPair(9.0)
  val dbl_10: (Double, Json) = makeTestPair(10.0)

  val dblListWithSum6: (String, List[Double]) = "price" -> List(1.0, 2.0, 3.0)
  val dblListWithSum9: (String, List[Double]) = "price" -> List(3.0, 3.0, 3.0)

  def overCheckForDouble: ColumnSumCheck = overCheck(dbl_8._2)
  def overCheckInclusiveForDouble: ColumnSumCheck = overCheckInclusive(dbl_9._2)
  def underCheckForDouble: ColumnSumCheck = underCheck(dbl_8._2)
  def betweenCheckForDouble: ColumnSumCheck = betweenCheck(dbl_2._2, dbl_10._2)

  // Helpers
  def overCheck(threshold: Json): ColumnSumCheck = ColumnSumCheck("price", Some(threshold))
  def overCheckInclusive(threshold: Json): ColumnSumCheck =
    ColumnSumCheck("price", Some(threshold), inclusive = Some(Json.fromBoolean(true)))
  def underCheck(threshold: Json): ColumnSumCheck = ColumnSumCheck("price", None, Some(threshold))
  def betweenCheck(lower: Json, upper: Json): ColumnSumCheck =
    ColumnSumCheck("price", Some(lower), Some(upper))
  def outsideCheck(lower: Json, upper: Json): ColumnSumCheck =
    ColumnSumCheck("price", Some(lower), Some(upper))
}

/**
  * In some parts of tests, we want an number while in others we need that number as Json.
  */
trait TestPairMakers {
  def makeTestPair(int: Int): (Int, Json) = (int, Json.fromInt(int))
  def makeTestPair(long: Long): (Long, Json) = (long, Json.fromLong(long))
  def makeTestPair(long: Double): (Double, Json) = (long, Json.fromDouble(long).get)
}
