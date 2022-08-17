package com.target.data_validator.stats

import com.target.TestingSparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SecondPassStatsAggregatorSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  describe("SecondPassStatsAggregator") {

    import spark.implicits._
    val data = NumericData.data.toDS

    it("should correctly calculate the standard deviation and histogram") {
      val stats1 = NumericData.firstPassStats
      val agg2 = new SecondPassStatsAggregator(stats1)

      val stats2 = data.select(agg2(data("value1")).as("stats"))
        .select(
          "stats.stdDev",
          "stats.histogram"
        )
        .as[SecondPassStats]
        .collect

      stats2.headOption match {
        case Some(s) =>
          assert(s.stdDev === NumericData.secondPassStats.stdDev)
          assert(s.histogram === NumericData.secondPassStats.histogram)
        case None => assert(false)
      }

    }

    it("should freely convert from spark Row type with the provided companion function") {
      val stats1 = NumericData.firstPassStats
      val agg2 = new SecondPassStatsAggregator(stats1)
      val outputRow = data.select(agg2(data("value1"))).head
      val outputStruct = outputRow.getStruct(0)

      SecondPassStats.fromRowRepr(outputStruct) shouldBe NumericData.secondPassStats
    }

  }

}
