package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkConfig, mkDict}
import com.target.data_validator.validator.{ColumnMaxCheck, MinNumRows, NegativeCheck, NullCheck}
import org.scalatest.{Matchers, WordSpec}

class ValidatorSpecifiedFormatLoaderSpec extends WordSpec with Matchers with TestingSparkSession {
  "ValidatorSpecifiedFormatLoader" should {
    "load json" in {
      val loader = ValidatorSpecifiedFormatLoader(
        format = "json",
        keyColumns = Some(List("age")),
        condition = None,
        checks = List(
          NegativeCheck("age", None),
          NullCheck("age", None),
          ColumnMaxCheck("age", JsonUtils.string2Json("68")),
          MinNumRows(JsonUtils.string2Json("9"))
        ),
        options = None,
        loadData = Some(List("src/test/resources/format_test.jsonl"))
      )

      val didFail = loader.quickChecks(spark, mkDict())(mkConfig(List(loader)))

      didFail should be(false)
      loader.getEvents should have size 2
      loader.getEvents
        .collectFirst { case vc: ValidatorCounter => vc }
        .get
        .value should be(9)
    }
  }
}
