package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.parser._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class RowBasedSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  def mkConfig(df: DataFrame, checks: List[ValidatorBase]): ValidatorConfig =
    ValidatorConfig(
      1,
      10, // scalastyle:ignore magic.number
      None,
      detailedErrors = false,
      None,
      None,
      List(ValidatorDataFrame(df, None, None, checks))
    )

  describe("NegativeCheck") {

    val schema = StructType(
      List(
        StructField("key", StringType),
        StructField("key2", StringType),
        StructField("data", IntegerType)
      )
    )

    it("should be able to be configured from json/YAML") {
      val json = """{ "type": "negativeCheck", "column": "med_regular_price" }"""
      assert(decode[ValidatorBase](json)(JsonDecoders.decodeChecks) == Right(NegativeCheck("med_regular_price", None)))
    }

    it("should fail with negative data") {
      val dict = new VarSubstitution
      val df = spark.createDataFrame(
        sc.parallelize(List(Row("negative", "one", -1), Row("zero", "zero", 0), Row("positive", "one", 1))),
        schema
      )
      val sut = mkConfig(df, List(NegativeCheck("data", None)))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
    }

    it("should not fail with non negative data") {
      val dict = new VarSubstitution
      val df = spark.createDataFrame(
        sc.parallelize(List(Row("zero", "one", 0), Row("positive", "one", 1), Row("more positive", "two", 2))),
        schema
      )
      val sut = mkConfig(df, List(NegativeCheck("data", None)))
      assert(!sut.configCheck(spark, dict), "configCheck() Failed!")
      assert(!sut.quickChecks(spark, dict), "quickChecks() Failed!")
      assert(!sut.failed)
    }

    it("negative data should be reported") {
      val dict = new VarSubstitution
      val df = spark.createDataFrame(
        sc.parallelize(List(Row("negative", "one", -1), Row("zero", "zero", 0), Row("positive", "one", 1))),
        schema
      )
      val vTable = ValidatorDataFrame(df, None, None, List(NegativeCheck("data", None)))
      val sut = mkConfig(df, List.empty).copy(tables = List(vTable))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(vTable.getEvents.exists(_.failed))
      assert(vTable.checks.flatMap(_.getEvents).exists(_.failed))
    }

    it("should fail when column DataType is StringType") {
      val dict = new VarSubstitution
      val badSchema = StructType(List(StructField("key", StringType), StructField("data", StringType)))
      val df = spark.createDataFrame(sc.parallelize(List(Row("number", "one"))), badSchema)
      val sut = mkConfig(df, List(NegativeCheck("data", None)))
      assert(sut.configCheck(spark, dict))
      assert(sut.failed)
    }

    it("should produce events with correct keyColumn data") {
      val dict = new VarSubstitution
      val df = spark.createDataFrame(
        sc.parallelize(List(Row("negative", "one", -1), Row("zero", "zero", 0), Row("positive", "one", 1))),
        schema
      )
      val vTable = ValidatorDataFrame(df, Some(List("key", "key2")), None, List(NegativeCheck("data", None)))
      val sut = mkConfig(df, List.empty).copy(detailedErrors = true, tables = List(vTable))
      assert(!sut.configCheck(spark, dict))
      assert(sut.quickChecks(spark, dict))
      assert(sut.failed)
      assert(vTable.getEvents.exists(_.failed))
      assert(vTable.checks.flatMap(_.getEvents) contains
        ValidatorQuickCheckError(
          List(("key", "negative"), ("key2", "one")),
          -1,
          "NegativeCheck failed! data = -1 and ('data < 0)"
        ))
    }

    it("variable substitution should produce VarSubJsonEvent()") {
      val vars = new VarSubstitution
      vars.addString("col", "junk")
      val sut = NullCheck("${col}", None).substituteVariables(vars)
      assert(!sut.failed)
      assert(sut.getEvents contains VarSubEvent("${col}", "junk"))
    }

    it("check on Double should work") {
      val dict = new VarSubstitution
      val schema = StructType(List(StructField("d", DoubleType)))
      val df = spark.createDataFrame(sc.parallelize(List(Row(-1.0), Row(0.0), Row(1.0))), schema)
      val sut = mkConfig(df, NegativeCheck("d", None) :: Nil)
      assert(sut.quickChecks(spark, dict))
    }

  }

  describe ("threshold tests") {
    describe ("validate different way of specifying thresholds") {
      it ("absolute 10") {
        val sut = NullCheck("col", Some("10"))
        assert(!sut.configCheckThreshold)
      }

      it ("less then 1.0") {
        val sut = NullCheck("col", Some("0.10"))
        assert(!sut.configCheckThreshold)
      }

      it ("10%") {
        val sut = NullCheck("col", Some("10%"))
        assert(!sut.configCheckThreshold)
      }

      it (" works with extra spacing before percentage sign") {
        val sut = NullCheck("col", Some("10%"))
        assert(!sut.configCheckThreshold)
        assert(sut.calcErrorCountThreshold(100) == 10) // scalastyle:ignore
      }

      it("bad value") {
        val sut = NullCheck("col", Some("peanuts"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }

      it ("is negative") {
        val sut = NullCheck("col", Some("-10"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }

      it ("is negative fraction") {
        val sut = NullCheck("col", Some("-0.1"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }
      it ("is negative percent") {
        val sut = NullCheck("col", Some("-10%"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }

      it ("negative percentage should be rejected") {
        val sut = NullCheck("col", Some("-2.3 %"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }

      it ("multiple '%' should be rejected") {
        val sut = NullCheck("col", Some("2.3 %%%"))
        assert(sut.configCheckThreshold)
        assert(sut.failed)
      }

    }

    describe ("calMaxErrors()") {
      val rowCount = 10000
      it ("absolute 10") {
        val sut = NullCheck("col", Some("10"))
        assert(sut.calcErrorCountThreshold(rowCount) == 10)
      }

      it ("less then 1.0") {
        val sut = NullCheck("col", Some("0.10"))
        assert(sut.calcErrorCountThreshold(rowCount) == 1000)
      }

      it ("10%") {
        val sut = NullCheck("col", Some("10%"))
        assert(sut.calcErrorCountThreshold(rowCount) == 1000)
      }

      it ("check for integer division") {
        val sut = NullCheck("col", Some("10%"))
        assert(sut.calcErrorCountThreshold(99) == 9) // scalastyle:ignore
      }
    }
  }

}
