package com.target.data_validator.stats

import com.target.TestingSparkSession
import org.scalatest.{FunSpec, Matchers}

class SecondPassStatsAggregatorSpec extends FunSpec with Matchers with TestingSparkSession {

  describe("SecondPassStatsAggregator") {

    it("should correctly calculate the standard deviation and histogram") {

      import spark.implicits._
      val data = NumericData.data.toDS

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

  }

}
