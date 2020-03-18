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
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: over
             |threshold: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "over", threshold = Some(int_8._2))))
      }
      it("uses under threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: under
             |threshold: ${int_8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "under", threshold = Some(int_8._2))))
      }
      it("uses between threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: between
             |lowerBound: ${int_2._1}
             |upperBound: ${int_10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "between",
            lowerBound = Some(int_2._2), upperBound = Some(int_10._2))))
      }
      it("uses outside threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: outside
             |lowerBound: ${int_2._1}
             |upperBound: ${int_10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "outside",
            lowerBound = Some(int_2._2), upperBound = Some(int_10._2))))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |thresholdType: over
             |threshold: ${int_8}
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
    eight = int_8._1, six = intListWithSum6, nine = intListWithSum9,
    under = underCheckForInt, over = overCheckForInt,
    between = betweenCheckForInt, outside = outsideCheckForInt
  )

  "SumOfNumericColumnCheck with longs" should behave like functionsCorrectly[Long](
    eight = long_8._1, six = longListWithSum6, nine = longListWithSum9,
    under = underCheckForLong, over = overCheckForLong,
    between = betweenCheckForLong, outside = outsideCheckForLong
  )

  "SumOfNumericColumnCheck with double" should behave like functionsCorrectly[Double](
    eight = dbl_8._1, six = dblListWithSum6, nine = dblListWithSum9,
    under = underCheckForLong, over = overCheckForLong,
    between = betweenCheckForLong, outside = outsideCheckForLong
  )

}

trait FunctionTestingForNumericalTypes
  extends TestingSparkSession
    with SumOfNumericColumnCheckBasicSetup { this: FlatSpec =>
  def functionsCorrectly[T: Numeric](
                          eight: T,
                          six: (String, List[T]),
                          nine: (String, List[T]),
                          under: SumOfNumericColumnCheck,
                          over: SumOfNumericColumnCheck,
                          between: SumOfNumericColumnCheck,
                          outside: SumOfNumericColumnCheck): Unit = {

    it should s"correctly check that ${nine._2.sum} is outside " +
      s"${outside.lowerBound.get.asNumber.get} and ${outside.upperBound.get.asNumber.get}" in {
      val df = mkDf(spark, nine) // scalastyle:ignore
      val sut = testDfWithChecks(df, outside)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${nine._2.sum} is between " +
      s"${between.lowerBound.get.asNumber.get} and ${between.upperBound.get.asNumber.get}" in {
      val df = mkDf(spark, nine) // scalastyle:ignore
      val sut = testDfWithChecks(df, between)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is not under ${six._2.sum}" in {
      val df = mkDf(spark, six) // scalastyle:ignore
      val sut = testDfWithChecks(df, under)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is not over ${nine._2.sum}" in {
      val df = mkDf(spark, nine) // scalastyle:ignore
      val sut = testDfWithChecks(df, over)
      assert(!sut.quickChecks(spark, mkDict())(config))
      assert(!sut.failed)
    }
    it should s"correctly check that ${eight} is over ${six._2.sum}" in {
      val df = mkDf(spark, six) // scalastyle:ignore
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
  val int_10: (Int, Json) = makeTestPair(10)

  val intListWithSum6: (String, List[Int]) = "price" -> List(1, 2, 3)
  val intListWithSum9: (String, List[Int]) = "price" -> List(3, 3, 3)

  def overCheckForInt: SumOfNumericColumnCheck = overCheck(int_8._2)
  def underCheckForInt: SumOfNumericColumnCheck = underCheck(int_8._2)
  def betweenCheckForInt: SumOfNumericColumnCheck = betweenCheck(int_2._2, int_10._2)
  def outsideCheckForInt: SumOfNumericColumnCheck = outsideCheck(int_2._2, int_8._2)

  // Long
  val long_8: (Long, Json) = makeTestPair(8L)
  val long_2: (Long, Json) = makeTestPair(2L)
  val long_10: (Long, Json) = makeTestPair(10L)

  val longListWithSum6: (String, List[Long]) = "price" -> List(1L, 2L, 3L)
  val longListWithSum9: (String, List[Long]) = "price" -> List(3L, 3L, 3L)

  def overCheckForLong: SumOfNumericColumnCheck = overCheck(long_8._2)
  def underCheckForLong: SumOfNumericColumnCheck = underCheck(long_8._2)
  def betweenCheckForLong: SumOfNumericColumnCheck = betweenCheck(long_2._2, long_10._2)
  def outsideCheckForLong: SumOfNumericColumnCheck = outsideCheck(long_2._2, long_8._2)

  // Double
  val dbl_8: (Double, Json) = makeTestPair(8.0)
  val dbl_2: (Double, Json) = makeTestPair(2.0)
  val dbl_10: (Double, Json) = makeTestPair(10.0)

  val dblListWithSum6: (String, List[Double]) = "price" -> List(1.0, 2.0, 3.0)
  val dblListWithSum9: (String, List[Double]) = "price" -> List(3.0, 3.0, 3.0)

  def overCheckForDouble: SumOfNumericColumnCheck = overCheck(dbl_8._2)
  def underCheckForDouble: SumOfNumericColumnCheck = underCheck(dbl_8._2)
  def betweenCheckForDouble: SumOfNumericColumnCheck = betweenCheck(dbl_2._2, dbl_10._2)
  def outsideCheckForDouble: SumOfNumericColumnCheck = outsideCheck(dbl_2._2, dbl_8._2)

  // Helpers
  def overCheck(threshold: Json): SumOfNumericColumnCheck = SumOfNumericColumnCheck("price", "over", Some(threshold))
  def underCheck(threshold: Json): SumOfNumericColumnCheck = SumOfNumericColumnCheck("price", "under", Some(threshold))
  def betweenCheck(lower: Json, upper: Json): SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "between", None, Some(lower), Some(upper))
  def outsideCheck(lower: Json, upper: Json): SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "outside", None, Some(lower), Some(upper))
}

/**
  * In some parts of tests, we want an number while in others we need that number as Json.
  */
trait TestPairMakers {
  def makeTestPair(int: Int): (Int, Json) = (int, Json.fromInt(int))
  def makeTestPair(long: Long): (Long, Json) = (long, Json.fromLong(long))
  def makeTestPair(long: Double): (Double, Json) = (long, Json.fromDouble(long).get)
}
