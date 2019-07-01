package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import com.target.data_validator.validator.ValidatorBase.D0
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class RangeCheckSpec extends FunSpec with Matchers with TestingSparkSession {
  val schema = StructType(
    List(
      StructField("item", StringType),
      StructField("min", DoubleType),
      StructField("avg", DoubleType),
      StructField("max", DoubleType)
    )
  )
  val defData = List(
    Row("Eggs", 2.99, 4.00, 5.99),
    Row("Milk", 5.35, 7.42, 10.27),
    Row("Bread", 1.00, 0.99, 1.01),
    Row("Cereal", 1.00, 1.10, 1.01)
  )

  def mkDataFrame(spark: SparkSession, data: List[Row]): DataFrame = spark.createDataFrame(sc.parallelize(data), schema)

  describe("RangeCheck") {

    describe("configCheck") {

      it("error if min and max are not defined") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck("item", None, None, None, None)
        assert(sut.configCheck(df))
      }

      it("error if min and max are different types") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck(
          "avg",
          Some(Json.fromInt(0)),
          Some(Json.fromString("ten")),
          None,
          None
        )
        assert(sut.configCheck(df))
        assert(sut.failed)
      }

      it("error if column is not found in df") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck(
          "bad_column_name",
          Some(Json.fromInt(0)),
          Some(Json.fromString("ten")),
          None,
          None
        )
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Column: 'bad_column_name' not found in schema."))
        assert(sut.failed)
      }

      it("error if col type is not equal value type") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck(
          "min",
          Some(Json.fromString("one")),
          Some(Json.fromString("100")),
          None,
          None
        )
        assert(sut.configCheck(df))
        assert(sut.failed)
      }

      it("minValue less than maxValue fails configCheck") {
        val dict = new VarSubstitution
        val maxValue = Math.abs(Random.nextInt(1000)) //scalastyle:ignore
        val minValue = maxValue + 10
        val sut = RangeCheck("avg", Some(Json.fromInt(minValue)), Some(Json.fromInt(maxValue)), None, None)
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError(s"Min: $minValue must be less than max: $maxValue"))
      }
    }

    describe("substitute vars") {

      it("variable column name isn't correct") {
        val dict = new VarSubstitution
        val sut = RangeCheck("$column", Json.fromDouble(0.0), None, None, None)
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
      }

      it("variable column name is good.") {
        val dict = new VarSubstitution
        dict.addString("column", "min")
        val minValue = Json.fromDouble(0.0)
        val sut = RangeCheck("$column", minValue, None, None, None)
        assert(sut.substituteVariables(dict) == RangeCheck("min", minValue, None, None, None))
        assert(!sut.failed)
      }

      it("substitutes minValue") {
        val dict = new VarSubstitution
        dict.addString("min_value", "0")
        val sut = RangeCheck("min", Some(Json.fromString("${min_value}")), None, None, None)
        assert(sut.substituteVariables(dict) == RangeCheck("min", Some(Json.fromInt(0)), None, None, None))
        assert(!sut.failed)
      }

      it("inclusive variable") {
        val dict = new VarSubstitution
        dict.add("inclusive", Json.fromBoolean(true))
        val sut = RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromString("$inclusive")),
          None)
        assert(sut.substituteVariables(dict) ==
          RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromBoolean(true)), None))
        assert(!sut.failed)
      }

      it("bad inclusive variable fails configCheck") {
        val dict = new VarSubstitution
        val sut = RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromInt(Random.nextInt)), None)
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
        assert(sut.failed)
      }

      it("threshold") {
        val dict = new VarSubstitution
        dict.add("threshold", Json.fromString("10%"))
        val sut = RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromBoolean(false)),
          Some("$threshold"))
        assert(sut.substituteVariables(dict) ==
          RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromBoolean(false)), Some("10%")))
        assert(!sut.failed)
      }
    }

    describe("colTest") {

      it("minValue") {
        val dict = new VarSubstitution
        val sut = RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromInt(Random.nextInt)), None)
        // TODO: Find better way to compare expressions.
        assert(sut.colTest(schema, dict).sql == LessThanOrEqual(UnresolvedAttribute("min"), D0).sql)
      }

      it("minValue inclusive") {
        val dict = new VarSubstitution
        val sut = RangeCheck("min", Some(Json.fromInt(0)), None, Some(Json.fromBoolean(true)), None)
        assert(sut.colTest(schema, dict).sql == LessThan(UnresolvedAttribute("min"), D0).sql)
      }

      it("maxValue") {
        val dict = new VarSubstitution
        val maxValue = Math.abs(Random.nextDouble)
        val sut = RangeCheck("max", None, Json.fromDouble(maxValue), None, None)
        // TODO: Find better way to compare expressions.
        assert(sut.colTest(schema, dict).sql ==
          GreaterThanOrEqual(UnresolvedAttribute("max"), Literal.create(maxValue, DoubleType)).sql)
      }

      it("maxValue inclusive") {
        val dict = new VarSubstitution
        val maxValue = Math.abs(Random.nextDouble)
        val sut = RangeCheck("avg", None, Json.fromDouble(maxValue), Some(Json.fromBoolean(true)), None)
        // TODO: Find better way to compare expressions.
        assert(sut.colTest(schema, dict).sql ==
          GreaterThan(UnresolvedAttribute("avg"), Literal.create(maxValue, DoubleType)).sql)
      }

      it("min and max value") {
        val dict = new VarSubstitution
        val maxValue = Math.abs(Random.nextDouble)
        val sut = RangeCheck("avg", Some(Json.fromInt(0)), Json.fromDouble(maxValue), None, None)
        // TODO: Find better way to compare expressions.
        assert(sut.colTest(schema, dict).sql ==
          Or(LessThanOrEqual(UnresolvedAttribute("avg"), D0),
            GreaterThanOrEqual(UnresolvedAttribute("avg"), Literal.create(maxValue, DoubleType))).sql)
      }
    }

    describe("RangeCheck.fromJson") {

      import JsonDecoders.decodeChecks
      val yaml =
        """---
          |- type: rangeCheck
          |  column: prices
          |  minValue: 0
          |  maxValue: 100
          |  inclusive: false
          |  threshold: 10%
          |
              """.stripMargin
      val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
      val sut = json.as[Array[ValidatorBase]]
      assert(sut.isRight)
      assert(sut.right.get contains RangeCheck(
        "prices",
        Some(Json.fromInt(0)),
        Some(Json.fromInt(100)), // scalastyle:ignore
        Some(Json.fromBoolean(false)),
        Some("10%")
      ))
    }

    describe("Basic Range check") {

      it("finds errors in data ") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck("max",
          Some(Json.fromInt(6)), Some(Json.fromInt(10)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(0, 0, None, detailedErrors = false, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "RangeCheck on column 'max'", defData.length, defData.length))
      }

      it ("detailed errors works") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck("max",
          Some(Json.fromInt(6)), Some(Json.fromInt(10)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(2, 1, None, detailedErrors = false, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "RangeCheck on column 'max'", defData.length, defData.length))
      }

      it ("report.json works") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck("max",
          Some(Json.fromInt(6)), Some(Json.fromInt(10)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(1, 1, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "RangeCheck on column 'max'", defData.length, defData.length))
        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "Eggs") :: Nil, 5.99,
            "RangeCheck failed! max = 5.99 and (('max <= 6.0) || ('max >= 10.0))"))
      }

      it("toJson works") {
        val minValue = Random.nextInt(1000) // scalastyle:ignore
        val minJson = Json.fromInt(minValue)
        val maxJson = Json.fromInt(minValue + Random.nextInt(1000) + 1) // scalastyle:ignore
        val sut = RangeCheck("max",
          Some(minJson), Some(maxJson), None, None) // scalastyle:ignore
        assert(sut.toJson == Json.obj(("type", Json.fromString("rangeCheck")),
          ("column", Json.fromString("max")),
          ("minValue", minJson),
          ("maxValue", maxJson),
          ("inclusive", Json.fromBoolean(false)),
          ("events", Json.arr())
        ))
      }
    }

    describe ("Support column names for min/max value") {

      it ("specifying column names for min/max value") {
        val sut = RangeCheck("avg", Some(Json.fromString("`min")),
          Some(Json.fromString("`max")), Some(Json.fromBoolean(true)), None)
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        assert(!sut.configCheck(df))
        assert(sut.colTest(df.schema, dict).sql == "((`avg` < `min`) OR (`avg` > `max`))")
      }

      it ("bad minValue column") {
        val sut = RangeCheck("avg", Some(Json.fromString("`minColumn")),
          Some(Json.fromString("max")), Some(Json.fromBoolean(true)), None)
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
      }

      it ("bad maxValue column") {
        val sut = RangeCheck("avg", Some(Json.fromString("`min")),
          Some(Json.fromString("`maxColumn")), Some(Json.fromBoolean(true)), None)
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
      }

      it ("full example") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = RangeCheck("avg", Some(Json.fromString("`min")), Some(Json.fromString("`max")), None, None)
        val config = ValidatorConfig(1, 1, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "RangeCheck on column 'avg'", defData.length, defData.length - 2))
        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "Bread") :: Nil, 0.99,
            "RangeCheck failed! avg = 0.99 and (('avg <= 'min) || ('avg >= 'max))"))
      }

    }

  }

}
