package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import io.circe._
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, FunSpec, Matchers}

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
             |type: columnSumCheck
             |column: foo
             |maxValue: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", maxValue = Some(int_8._2))))
      }
      it("uses under threshold") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", minValue = Some(int_8._2))))
      }
      it("uses between threshold") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_2._1}
             |maxValue: ${int_10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo",
            minValue = Some(int_2._2), maxValue = Some(int_10._2))))
      }
      it("uses outside threshold") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |column: foo
             |minValue: ${int_2._1}
             |maxValue: ${int_10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo",
            minValue = Some(int_2._2), maxValue = Some(int_10._2))))
      }

      it("uses over threshold, inclusively") {
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
          SumOfNumericColumnCheck("foo", maxValue = Some(int_8._2), inclusive = Some(Json.fromBoolean(true)))))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: columnSumCheck
             |maxValue: ${int_8}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut.isLeft)
      }
    }

    describe("variable substitution") {
      it("success substitution") {
        val dict = mkDict("minValue" -> "20", "column" -> "foo")
        val sut = SumOfNumericColumnCheck("$column", Some(Json.fromString("$minValue")))
        assert(sut.substituteVariables(dict) ==
          SumOfNumericColumnCheck("foo", Some(Json.fromInt(20))))
        assert(!sut.failed)
      }

      it("error on substitution issues") {
        val dict = mkDict()
        val sut = SumOfNumericColumnCheck("$column", Some(Json.fromString("$minValue")))
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorError("VariableSubstitution: Can't find values for the following keys, "
            + "column"))
      }
    }

    describe("check configuration") {
      it("errors without a min or max declared") {
        val df = mkDf(spark = spark, "price" -> List(1.99))
        val sut = SumOfNumericColumnCheck("price")
        assert(sut.configCheck(df))
      }

      it("Column Exists") {
        val df = mkDf(spark = spark, "price" -> List(1.99))
        val sut = overCheckForInt
        assert(!sut.configCheck(df))
      }

      it("Column doesn't exist") {
        val df = mkDf(spark = spark, ("lolnope" -> List(1.99)))
        val sut = overCheckForInt
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price not found in schema."))
      }

      it("Column exists but is wrong type") {
        val df = mkDf(spark = spark, ("price" -> List("eggs")))
        val sut = overCheckForInt
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError("Column: price found, but not of numericType type: StringType"))
      }
    }
  }
}

class SumOfNumericColumnCheckFunctionalSpec
  extends FlatSpec
    with FunctionTestingForNumericalTypes
    with SumOfNumericColumnCheckExamples
{

  "SumOfNumericColumnCheck with integers" should behave like functionsCorrectly[Int](
    eight = int_8._1, nine = int_9._1, sixPair = intListWithSum6, ninePair = intListWithSum9,
    under = underCheckForInt, over = overCheckForInt,
    between = betweenCheckForInt,
    overInclusive = overCheckInclusiveForInt
  )

  "SumOfNumericColumnCheck with longs" should behave like functionsCorrectly[Long](
    eight = long_8._1, nine = long_9._1, sixPair = longListWithSum6, ninePair = longListWithSum9,
    under = underCheckForLong, over = overCheckForLong,
    between = betweenCheckForLong,
    overInclusive = overCheckInclusiveForLong
  )

  "SumOfNumericColumnCheck with double" should behave like functionsCorrectly[Double](
    eight = dbl_8._1, nine = dbl_9._1, sixPair = dblListWithSum6, ninePair = dblListWithSum9,
    under = underCheckForLong, over = overCheckForLong,
    between = betweenCheckForLong,
    overInclusive = overCheckInclusiveForDouble
  )

}

trait FunctionTestingForNumericalTypes
  extends TestingSparkSession
    with SumOfNumericColumnCheckBasicSetup { this: FlatSpec =>
  def functionsCorrectly[T: Numeric](
                                      eight: T,
                                      nine: T,
                                      sixPair: (String, List[T]),
                                      ninePair: (String, List[T]),
                                      under: SumOfNumericColumnCheck,
                                      over: SumOfNumericColumnCheck,
                                      between: SumOfNumericColumnCheck,
                                      overInclusive: SumOfNumericColumnCheck
                                    ): Unit = {
    it should s"correctly check that ${ninePair._2.sum} is between " +
      s"${between.minValue.get.asNumber.get} and ${between.maxValue.get.asNumber.get}" in {
      val df = mkDf(spark, ninePair) // scalastyle:ignore
      val sut = testDfWithChecks(df, between)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is not under ${sixPair._2.sum}" in {
      val df = mkDf(spark, sixPair) // scalastyle:ignore
      val sut = testDfWithChecks(df, under)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is not over ${ninePair._2.sum}" in {
      val df = mkDf(spark, ninePair) // scalastyle:ignore
      val sut = testDfWithChecks(df, over)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${nine} is not over ${ninePair._2.sum} in inclusive mode" in {
      val df = mkDf(spark, ninePair) // scalastyle:ignore
      val sut = testDfWithChecks(df, overInclusive)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is over ${sixPair._2.sum}" in {
      val df = mkDf(spark, sixPair) // scalastyle:ignore
      val sut = testDfWithChecks(df, over)
      assert(sut.quickChecks(spark, mkDict())(config))
      assert(sut.failed)
    }
  }
}


trait SumOfNumericColumnCheckBasicSetup {
  val useDetailedErrors = false
  val config: ValidatorConfig = ValidatorConfig(
    numKeyCols = 1,
    numErrorsToReport = 1,
    email = None,
    detailedErrors = useDetailedErrors,
    vars = None,
    outputs = None,
    tables = List.empty
  )

  def testDfWithChecks(df: DataFrame, checks: ValidatorBase*): ValidatorDataFrame = {
    ValidatorDataFrame(df, None, None, checks.toList)
  }
}
trait SumOfNumericColumnCheckExamples extends TestPairMakers {
  // Int
  val int_8: (Int, Json) = makeTestPair(8)
  val int_2: (Int, Json) = makeTestPair(2)
  val int_9: (Int, Json) = makeTestPair(9)
  val int_10: (Int, Json) = makeTestPair(10)

  val intListWithSum6: (String, List[Int]) = "price" -> List(1, 2, 3)
  val intListWithSum9: (String, List[Int]) = "price" -> List(3, 3, 3)

  def overCheckForInt: SumOfNumericColumnCheck = overCheck(int_8._2)
  def overCheckInclusiveForInt: SumOfNumericColumnCheck = overCheckInclusive(int_9._2)
  def underCheckForInt: SumOfNumericColumnCheck = underCheck(int_8._2)
  def betweenCheckForInt: SumOfNumericColumnCheck = betweenCheck(int_2._2, int_10._2)

  // Long
  val long_8: (Long, Json) = makeTestPair(8L)
  val long_2: (Long, Json) = makeTestPair(2L)
  val long_9: (Long, Json) = makeTestPair(9L)
  val long_10: (Long, Json) = makeTestPair(10L)

  val longListWithSum6: (String, List[Long]) = "price" -> List(1L, 2L, 3L)
  val longListWithSum9: (String, List[Long]) = "price" -> List(3L, 3L, 3L)

  def overCheckForLong: SumOfNumericColumnCheck = overCheck(long_8._2)
  def overCheckInclusiveForLong: SumOfNumericColumnCheck = overCheckInclusive(long_9._2)
  def underCheckForLong: SumOfNumericColumnCheck = underCheck(long_8._2)
  def betweenCheckForLong: SumOfNumericColumnCheck = betweenCheck(long_2._2, long_10._2)

  // Double
  val dbl_8: (Double, Json) = makeTestPair(8.0)
  val dbl_2: (Double, Json) = makeTestPair(2.0)
  val dbl_9: (Double, Json) = makeTestPair(9.0)
  val dbl_10: (Double, Json) = makeTestPair(10.0)

  val dblListWithSum6: (String, List[Double]) = "price" -> List(1.0, 2.0, 3.0)
  val dblListWithSum9: (String, List[Double]) = "price" -> List(3.0, 3.0, 3.0)

  def overCheckForDouble: SumOfNumericColumnCheck = overCheck(dbl_8._2)
  def overCheckInclusiveForDouble: SumOfNumericColumnCheck = overCheckInclusive(dbl_9._2)
  def underCheckForDouble: SumOfNumericColumnCheck = underCheck(dbl_8._2)
  def betweenCheckForDouble: SumOfNumericColumnCheck = betweenCheck(dbl_2._2, dbl_10._2)

  // Helpers
  def overCheck(threshold: Json): SumOfNumericColumnCheck = SumOfNumericColumnCheck("price", Some(threshold))
  def overCheckInclusive(threshold: Json): SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", Some(threshold), inclusive = Some(Json.fromBoolean(true)))
  def underCheck(threshold: Json): SumOfNumericColumnCheck = SumOfNumericColumnCheck("price", None, Some(threshold))
  def betweenCheck(lower: Json, upper: Json): SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", Some(lower), Some(upper))
  def outsideCheck(lower: Json, upper: Json): SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", Some(lower), Some(upper))
}

/**
  * In some parts of tests, we want an number while in others we need that number as Json.
  */
trait TestPairMakers {
  def makeTestPair(int: Int): (Int, Json) = (int, Json.fromInt(int))
  def makeTestPair(long: Long): (Long, Json) = (long, Json.fromLong(long))
  def makeTestPair(long: Double): (Double, Json) = (long, Json.fromDouble(long).get)
}
