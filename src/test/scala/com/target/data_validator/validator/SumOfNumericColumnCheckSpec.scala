package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ValidatorConfig, ValidatorDataFrame, ValidatorError}
import com.target.data_validator.TestHelpers.{mkDf, mkDict, parseYaml}
import io.circe._
import org.apache.spark.sql.DataFrame
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
             |threshold: ${expectedThreshold8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "over", threshold = Some(expectedThreshold8._2))))
      }
      it("uses under threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: under
             |threshold: ${expectedThreshold8._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "under", threshold = Some(expectedThreshold8._2))))
      }
      it("uses between threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: between
             |lowerBound: ${expectedLower2._1}
             |upperBound: ${expectedUpper10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "between",
            lowerBound = Some(expectedLower2._2), upperBound = Some(expectedUpper10._2))))
      }
      it("uses outside threshold") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |column: foo
             |thresholdType: outside
             |lowerBound: ${expectedLower2._1}
             |upperBound: ${expectedUpper10._1}
             |""".stripMargin
        )
        val sut = JsonDecoders.decodeChecks.decodeJson(json)
        assert(sut == Right(
          SumOfNumericColumnCheck("foo", "outside",
            lowerBound = Some(expectedLower2._2), upperBound = Some(expectedUpper10._2))))
      }

      it("is missing the column") {
        val json = parseYaml(
          s"""
             |type: sumOfNumericColumnCheck
             |thresholdType: over
             |threshold: ${expectedThreshold8}
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
      it(s"correctly checks that ${expectedThreshold8._1} is not under ${listWithSum6._2.sum}") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = testDfWithChecks(df, underCheck)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }
      it(s"correctly checks that ${expectedThreshold8._1} is not over ${listWithSum9._2.sum}") {
        val df = mkDf(spark, listWithSum9) // scalastyle:ignore
        val sut = testDfWithChecks(df, overCheck)
        assert(!sut.quickChecks(spark, mkDict())(config))
        assert(!sut.failed)
      }
      it(s"correctly checks that ${expectedThreshold8._1} is over ${listWithSum6._2.sum}") {
        val df = mkDf(spark, listWithSum6) // scalastyle:ignore
        val sut = testDfWithChecks(df, overCheck)
        assert(sut.quickChecks(spark, mkDict())(config))
        assert(sut.failed)
      }
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
trait SumOfNumericColumnCheckExamples extends JsonHelpers {
  val expectedThreshold8: (Int, Json) = makeTestPair(8)
  val expectedLower2: (Int, Json) = makeTestPair(2)
  val expectedUpper10: (Int, Json) = makeTestPair(10)

  val listWithSum6: (String, List[Int]) = "price" -> List(1, 2, 3)
  val listWithSum9: (String, List[Int]) = "price" -> List(3, 3, 3)

  def overCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "over", threshold = Some(expectedThreshold8._2))
  def underCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "under", threshold = Some(expectedThreshold8._2))
  def betweenCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "between",
    lowerBound = Some(expectedLower2._2), upperBound = Some(expectedUpper10._2))
  def outsideCheck: SumOfNumericColumnCheck =
    SumOfNumericColumnCheck("price", "outside",
    lowerBound = Some(expectedLower2._2), upperBound = Some(expectedUpper10._2))
}

trait JsonHelpers {
  /**
    * In some parts of tests, we want an number while in others we need that number as Json
    * @param int
    * @return
    */
  def makeTestPair(int: Int): (Int, Json) = (int, Json.fromInt(int))
}
