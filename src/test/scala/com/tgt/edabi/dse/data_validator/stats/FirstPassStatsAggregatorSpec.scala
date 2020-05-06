package com.tgt.edabi.dse.data_validator.stats

import com.target.data_validator.stats.{FirstPassStats, FirstPassStatsAggregator}
import com.tgt.edabi.dse.TestingSparkSession
import org.scalatest.{FunSpec, Matchers}

class FirstPassStatsAggregatorSpec extends FunSpec with Matchers with TestingSparkSession {

  describe("FirstPassStatsAggregator") {

    it("should correctly calculate the count, mean, min and max values") {

      import spark.implicits._
      val data = NumericData.data.toDS

      val agg1 = new FirstPassStatsAggregator
      val stats = data.select(agg1(data("value1")).as("stats"))
        .select(
          "stats.count",
          "stats.mean",
          "stats.min",
          "stats.max"
        )
        .as[FirstPassStats]
        .collect

      stats.headOption match {
        case Some(s) =>
          assert(s.count === NumericData.firstPassStats.count)
          assert(s.mean  === NumericData.firstPassStats.mean)
          assert(s.min   === NumericData.firstPassStats.min)
          assert(s.max   === NumericData.firstPassStats.max)
        case None => assert(false)
      }

    }

  }

}