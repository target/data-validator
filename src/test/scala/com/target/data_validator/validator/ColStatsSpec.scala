package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import com.target.data_validator.stats._
import io.circe.Json
import org.scalatest._

// scalastyle:off magic.number
class ColStatsSpec extends FunSpec with Matchers with TestingSparkSession {
  import spark.implicits._

  describe("ColStats + ValidatorDataFrame") {

    val variables = new VarSubstitution
    val sampleDS = spark.createDataset(ColStatsSpec.sample)
    val validatorTable = ValidatorDataFrame(
      df = sampleDS.toDF,
      checks = List(
        new ColStats("a"),
        new ColStats("b"),
        new NullCheck("a", None),
        new NullCheck("b", None),
        new ColumnSumCheck("a", minValue = Some(Json.fromInt(0))),
        new ColumnSumCheck("b", minValue = Some(Json.fromInt(0)))
      ),
      keyColumns = None,
      condition = None
    )
    val validatorConfig = ValidatorConfig(0, 5, None, true, None, None, List(validatorTable))

    it("should run ColStats alongside other row and column based checks without error") {
      validatorConfig.quickChecks(spark, variables) shouldBe false
      validatorConfig.costlyChecks(spark, variables) shouldBe false
    }

    it("should generate the appropriate ColStats entries in report.json") {
      val report = validatorConfig.genJsonReport(variables)(spark)
      val summaries = report \\ "events" flatMap { json =>
        json.as[Seq[CompleteStats]] match {
          case Right(summary) => summary
          case _ => Seq.empty
        }
      }

      summaries.toSet shouldBe Set(ColStatsSpec.statsA, ColStatsSpec.statsB)
    }

  }

}

object ColStatsSpec {

  case class Sample(a: Long, b: Double)

  val sample = Seq(
    Sample(2, 0.3922),
    Sample(3, 0.4765),
    Sample(4, 0.1918),
    Sample(5, 0.0536),
    Sample(6, 0.4949),
    Sample(7, 0.5810),
    Sample(8, 0.2978),
    Sample(9, 0.0729),
    Sample(10, 0.868),
    Sample(11, 0.325),
    Sample(12, 0.305),
    Sample(13, 0.217),
    Sample(14, 0.193),
    Sample(15, 0.405),
    Sample(16, 0.443),
    Sample(17, 0.103),
    Sample(18, 0.435),
    Sample(19, 0.953),
    Sample(20, 0.519),
    Sample(21, 0.958)
  )

  val statsA = CompleteStats(
    "`a` stats",
    "a",
    20,
    11.5,
    2.0,
    21.0,
    5.916079783099616,
    Histogram(
      Seq(
        Bin(2.0, 3.9, 2),
        Bin(3.9, 5.8, 2),
        Bin(5.8, 7.699999999999999, 2),
        Bin(7.699999999999999, 9.6, 2),
        Bin(9.6, 11.5, 2),
        Bin(11.5, 13.399999999999999, 2),
        Bin(13.399999999999999, 15.299999999999999, 2),
        Bin(15.299999999999999, 17.2, 2),
        Bin(17.2, 19.099999999999998, 2),
        Bin(19.099999999999998, 21.0, 2)
      )
    )
  )

  val statsB = CompleteStats(
    "`b` stats",
    "b",
    20,
    0.414235,
    0.0536,
    0.958,
    0.26725316654123255,
    Histogram(
      Seq(
        Bin(0.0536, 0.14404, 3),
        Bin(0.14404, 0.23448, 3),
        Bin(0.23448, 0.32492, 2),
        Bin(0.32492, 0.41535999999999995, 3),
        Bin(0.41535999999999995, 0.5057999999999999, 4),
        Bin(0.5057999999999999, 0.59624, 2),
        Bin(0.59624, 0.68668, 0),
        Bin(0.68668, 0.7771199999999999, 0),
        Bin(0.7771199999999999, 0.8675599999999999, 0),
        Bin(0.8675599999999999, 0.958, 3)
      )
    )
  )

}
