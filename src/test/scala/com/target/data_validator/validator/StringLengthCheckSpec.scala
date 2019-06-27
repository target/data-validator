package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class StringLengthCheckSpec  extends FunSpec with Matchers with TestingSparkSession {

  val schema = StructType(
    List(
      StructField("item", StringType),
      StructField("baseprice", DoubleType)
    )
  )

  val defData = List(
    Row("Item1", 2.99),
    Row("Item23", 5.35),
    Row("I", 1.00),
    Row("", 1.00)
  )

  def mkDataFrame(spark: SparkSession, data: List[Row]): DataFrame = spark.createDataFrame(sc.parallelize(data), schema)

  describe("StringLengthCheck") {

    describe("configCheck") {

      it("error if min and max are not defined") {
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, None, None)
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Must define minValue or maxValue or both."))
        assert(sut.failed)
      }

      it("error if min and max are different types") {
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck(
          "item",
          Some(Json.fromInt(0)),
          Some(Json.fromString("ten")),
          None
        )
        assert(sut.configCheck(df))
        assert(sut.failed)
      }

      it("error if column is not found in df") {
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck(
          "bad_column_name",
          Some(Json.fromInt(0)),
          Some(Json.fromString("ten")),
          None
        )
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Column: 'bad_column_name' not found in schema."))
        assert(sut.failed)
      }

      it("error if col type is not String") {
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck(
          "baseprice",
          Some(Json.fromString("one")),
          Some(Json.fromString("100")),
          None
        )
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Data type of column 'baseprice' must be String, but was found to be DoubleType"))
        assert(sut.failed)
      }

      it("minValue less than maxValue fails configCheck") {
        val maxValue = Math.abs(Random.nextInt(1000)) //scalastyle:ignore
        val minValue = maxValue + 10
        val sut = StringLengthCheck("item", Some(Json.fromInt(minValue)), Some(Json.fromInt(maxValue)), None)
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(sut.getEvents contains ValidatorError(s"min: $minValue must be less than or equal to max: $maxValue"))
      }

    }

    describe("substitute vars") {

      it("variable column name isn't correct") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("$column", Some(Json.fromInt(1)), None, None)
        assert(sut.substituteVariables(dict) == sut)
        assert(sut.failed)
      }

      it("variable column name is good.") {
        val dict = new VarSubstitution
        dict.addString("column", "item")
        val minValue = Json.fromDouble(0.0)
        val sut = StringLengthCheck("$column", minValue, None, None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", minValue, None, None))
        assert(!sut.failed)
      }

      it("substitutes minValue") {
        val dict = new VarSubstitution
        dict.addString("minValue", "0")
        val sut = StringLengthCheck("item", Some(Json.fromString("${minValue}")), None, None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", Some(Json.fromInt(0)), None, None))
        assert(!sut.failed)
      }

      it("substitutes maxValue") {
        val dict = new VarSubstitution
        dict.addString("maxValue", "10")
        val sut = StringLengthCheck("item", None, Some(Json.fromString("${maxValue}")), None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", None, Some(Json.fromInt(10)), None))
        assert(!sut.failed)
      }

      it("substitutes minValue and maxValue") {
        val dict = new VarSubstitution
        dict.addString("minValue", "1")
        dict.addString("maxValue", "10")
        val sut = StringLengthCheck("item", Some(Json.fromString("${minValue}")),
          Some(Json.fromString("${maxValue}")), None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", Some(Json.fromInt(1)),
          Some(Json.fromInt(10)), None)) // scalastyle: ignore
        assert(!sut.failed)
      }

      it("substitutes threshold") {
        val dict = new VarSubstitution
        // scalastyle:off
        val threshold = Json.fromInt(100)
        val minValue = Some(Json.fromInt(1))
        val maxValue = Some(Json.fromInt(10))
        // scalastyle:on
        dict.add("threshold", threshold)
        val sut = StringLengthCheck("item", minValue, maxValue, Some("${threshold}"))
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", minValue, maxValue,
           Some("100"))) // scalastyle: ignore
        assert(!sut.failed)
      }
    }

    describe("colTest") {

      it("minValue") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", Some(Json.fromInt(2)), None, None)
        assert(sut.colTest(schema, dict).sql ==
          LessThan(Length(UnresolvedAttribute("item")), Literal.create(2, IntegerType)).sql)
      }

      it("maxValue") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(2)), None)
        assert(sut.colTest(schema, dict).sql ==
          GreaterThan(Length(UnresolvedAttribute("item")), Literal.create(2, IntegerType)).sql)
      }

      it("minValue and maxValue") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", Some(Json.fromInt(1)), Some(Json.fromInt(10)), None) // scalastyle:ignore
        assert(sut.colTest(schema, dict).sql ==
          Or(LessThan(Length(UnresolvedAttribute("item")), Literal.create(1, IntegerType)),
            GreaterThan(Length(UnresolvedAttribute("item")), Literal.create(10, IntegerType))).sql)
      }

    }

    describe("StringLengthCheck.fromJson") {

      it("fromJson with min and max") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  minValue: 0
            |  maxValue: 100
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringLengthCheck(
          "item",
          Some(Json.fromInt(0)),
          Some(Json.fromInt(100)), // scalastyle:ignore
          None
        ))
      }

      it("fromJson with min") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  minValue: 0
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringLengthCheck(
          "item",
          Some(Json.fromInt(0)),
          None,
          None
        ))
      }

      it("fromJson with max") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  maxValue: 100
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringLengthCheck(
          "item",
          None,
          Some(Json.fromInt(100)), // scalastyle:ignore
          None
        ))
      }

      it("fromJson without min or max") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringLengthCheck(
          "item",
          None,
          None,
          None
        ))
      }
    }

    describe("Basic String Length check") {

      it ("String length check fails numErrorsToReport:1") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 1, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)) // scalastyle:ignore

        // There are 2 invalid rows, and numErrorsToReport is
        // 1. So we need to check that exactly one of the 2 errors are present
        assert((sut.getEvents contains
                ValidatorQuickCheckError(("item", "I") :: Nil, "I",
                  "StringLengthCheck failed! item = I and ((length('item) < 5) || (length('item) > 6))")) ^
               (sut.getEvents contains
                 ValidatorQuickCheckError(("item", "") :: Nil, "",
                   "StringLengthCheck failed! item =  and ((length('item) < 5) || (length('item) > 6))"))
             )
      }

      it ("String length check fails numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)) //scalastyle:ignore

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "I") :: Nil, "I",
            "StringLengthCheck failed! item = I and ((length('item) < 5) || (length('item) > 6))"))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "") :: Nil, "",
            "StringLengthCheck failed! item =  and ((length('item) < 5) || (length('item) > 6))"))
      }

      it ("String length check fails for minValue = maxValue and numErrorsToReport:3") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(5)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 3, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 3)) // scalastyle:ignore

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "I") :: Nil, "I",
            "StringLengthCheck failed! item = I and ((length('item) < 5) || (length('item) > 5))"))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "") :: Nil, "",
            "StringLengthCheck failed! item =  and ((length('item) < 5) || (length('item) > 5))"))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "Item23") :: Nil, "Item23",
            "StringLengthCheck failed! item = Item23 and ((length('item) < 5) || (length('item) > 5))"))
      }

      it ("String length check fails for only minValue specified and numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)) // scalastyle:ignore

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "I") :: Nil, "I",
            "StringLengthCheck failed! item = I and (length('item) < 5)"))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "") :: Nil, "",
            "StringLengthCheck failed! item =  and (length('item) < 5)"))
      }

      it ("String length check fails for only maxValue specified and numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(2)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "Item1") :: Nil, "Item1",
            "StringLengthCheck failed! item = Item1 and (length('item) > 2)"))

        assert(sut.getEvents contains
          ValidatorQuickCheckError(("item", "Item23") :: Nil, "Item23",
            "StringLengthCheck failed! item = Item23 and (length('item) > 2)"))
      }

      it ("String length check passes for minValue and  maxValue specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(0)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)) // scalastyle:ignore
      }

      it ("String length check passes for only minValue specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(0)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)) // scalastyle:ignore
      }

      it ("String length check passes for only maxValue specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(1, 2, None, detailedErrors = true, None,
          None, List(ValidatorDataFrame(df, None, None, sut :: Nil)))
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(sut.getEvents contains
          ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)) // scalastyle:ignore
      }

      it("toJson works") {
        val minValue = Random.nextInt(1000) // scalastyle:ignore
        val minJson = Json.fromInt(minValue)
        val maxJson = Json.fromInt(minValue + Random.nextInt(1000) + 1) // scalastyle:ignore
        val sut = StringLengthCheck("item", Some(minJson), Some(maxJson), None) // scalastyle:ignore
        assert(sut.toJson == Json.obj(("type", Json.fromString("stringLengthCheck")),
          ("column", Json.fromString("item")),
          ("minValue", minJson),
          ("maxValue", maxJson),
          ("events", Json.arr())
        ))
      }
    }
  }
}