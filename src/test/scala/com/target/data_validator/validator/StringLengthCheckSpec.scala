package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.util.Random
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class StringLengthCheckSpec extends AnyFunSpec with Matchers with TestingSparkSession {

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

  def mkDataFrame(spark: SparkSession, data: List[Row]): DataFrame =
    spark.createDataFrame(sc.parallelize(data), schema)

  describe("StringLengthCheck") {

    describe("configCheck") {

      it("error if min and max are not defined") {
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, None, None)
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Must define minLength or maxLength or both."))
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
        assert(
          sut.getEvents contains
            ValidatorError("Data type of column 'baseprice' must be String, but was found to be DoubleType")
        )
        assert(sut.failed)
      }

      it("minLength less than maxLength fails configCheck") {
        val maxLength = Math.abs(Random.nextInt(1000)) // scalastyle:ignore
        val minLength = maxLength + 10
        val sut = StringLengthCheck("item", Some(Json.fromInt(minLength)), Some(Json.fromInt(maxLength)), None)
        val df = mkDataFrame(spark, defData)
        assert(sut.configCheck(df))
        assert(sut.failed)
        assert(
          sut.getEvents contains ValidatorError(s"min: $minLength must be less than or equal to max: $maxLength")
        )
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
        val minLength = Json.fromDouble(0.0)
        val sut = StringLengthCheck("$column", minLength, None, None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", minLength, None, None))
        assert(!sut.failed)
      }

      it("substitutes minLength") {
        val dict = new VarSubstitution
        dict.addString("minLength", "0")
        val sut = StringLengthCheck("item", Some(Json.fromString("${minLength}")), None, None)
        assert(sut.substituteVariables(dict) == StringLengthCheck("item", Some(Json.fromInt(0)), None, None))
        assert(!sut.failed)
      }

      it("substitutes maxLength") {
        val dict = new VarSubstitution
        dict.addString("maxLength", "10")
        val sut = StringLengthCheck("item", None, Some(Json.fromString("${maxLength}")), None)
        assert(
          sut.substituteVariables(dict) ==
            StringLengthCheck("item", None, Some(Json.fromInt(10)), None)
        ) // scalastyle:ignore
        assert(!sut.failed)
      }

      it("substitutes minLength and maxLength") {
        val dict = new VarSubstitution
        dict.addString("minLength", "1")
        dict.addString("maxLength", "10")
        val sut = StringLengthCheck(
          "item",
          Some(Json.fromString("${minLength}")),
          Some(Json.fromString("${maxLength}")),
          None
        )
        assert(
          sut.substituteVariables(dict) == StringLengthCheck(
            "item",
            Some(Json.fromInt(1)),
            Some(Json.fromInt(10)),
            None
          )
        ) // scalastyle:ignore
        assert(!sut.failed)
      }

      it("substitutes threshold") {
        val dict = new VarSubstitution
        // scalastyle:off
        val threshold = Json.fromInt(100)
        val minLength = Some(Json.fromInt(1))
        val maxLength = Some(Json.fromInt(10))
        // scalastyle:on
        dict.add("threshold", threshold)
        val sut = StringLengthCheck("item", minLength, maxLength, Some("${threshold}"))
        assert(
          sut.substituteVariables(dict) == StringLengthCheck("item", minLength, maxLength, Some("100"))
        ) // scalastyle: ignore
        assert(!sut.failed)
      }
    }

    describe("colTest") {

      it("minLength") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", Some(Json.fromInt(2)), None, None)
        assert(
          sut.colTest(schema, dict).sql ==
            LessThan(Length(UnresolvedAttribute("item")), Literal.create(2, IntegerType)).sql
        )
      }

      it("maxLength") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(2)), None)
        assert(
          sut.colTest(schema, dict).sql ==
            GreaterThan(Length(UnresolvedAttribute("item")), Literal.create(2, IntegerType)).sql
        )
      }

      it("minLength and maxLength") {
        val dict = new VarSubstitution
        val sut = StringLengthCheck("item", Some(Json.fromInt(1)), Some(Json.fromInt(10)), None) // scalastyle:ignore
        assert(
          sut.colTest(schema, dict).sql ==
            Or(
              LessThan(Length(UnresolvedAttribute("item")), Literal.create(1, IntegerType)),
              GreaterThan(Length(UnresolvedAttribute("item")), Literal.create(10, IntegerType))
            ).sql
        ) // scalastyle:ignore
      }

    }

    describe("StringLengthCheck.fromJson") {

      it("fromJson with min and max") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  minLength: 0
            |  maxLength: 100
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(
          sut.right.get contains StringLengthCheck(
            "item",
            Some(Json.fromInt(0)),
            Some(Json.fromInt(100)), // scalastyle:ignore
            None
          )
        )
      }

      it("fromJson with min") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  minLength: 0
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(
          sut.right.get contains StringLengthCheck(
            "item",
            Some(Json.fromInt(0)),
            None,
            None
          )
        )
      }

      it("fromJson with max") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringLengthCheck
            |  column: item
            |  maxLength: 100
            |
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(
          sut.right.get contains StringLengthCheck(
            "item",
            None,
            Some(Json.fromInt(100)), // scalastyle:ignore
            None
          )
        )
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
        assert(
          sut.right.get contains StringLengthCheck(
            "item",
            None,
            None,
            None
          )
        )
      }
    }

    describe("Basic String Length check") {

      it("String length check fails numErrorsToReport:1") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          1,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)
        ) // scalastyle:ignore

        // There are 2 invalid rows, and numErrorsToReport is
        // 1. So we need to check that exactly one of the 2 errors are present
        assert(
          (sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringLengthCheck failed! item = I and ((length('item) < 5) OR (length('item) > 6))"
            )) ^
            (sut.getEvents contains
              ValidatorQuickCheckError(
                ("item", "") :: Nil,
                "",
                "StringLengthCheck failed! item =  and ((length('item) < 5) OR (length('item) > 6))"
              ))
        )
      }

      it("String length check fails numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringLengthCheck failed! item = I and ((length('item) < 5) OR (length('item) > 6))"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "") :: Nil,
              "",
              "StringLengthCheck failed! item =  and ((length('item) < 5) OR (length('item) > 6))"
            )
        )
      }

      it("String length check fails for minLength = maxLength and numErrorsToReport:3") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), Some(Json.fromInt(5)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          3,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 3)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringLengthCheck failed! item = I and ((length('item) < 5) OR (length('item) > 5))"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "") :: Nil,
              "",
              "StringLengthCheck failed! item =  and ((length('item) < 5) OR (length('item) > 5))"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "Item23") :: Nil,
              "Item23",
              "StringLengthCheck failed! item = Item23 and ((length('item) < 5) OR (length('item) > 5))"
            )
        )
      }

      it("String length check fails for only minLength specified and numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(5)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringLengthCheck failed! item = I and (length('item) < 5)"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "") :: Nil,
              "",
              "StringLengthCheck failed! item =  and (length('item) < 5)"
            )
        )
      }

      it("String length check fails for only maxLength specified and numErrorsToReport:2") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(2)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(config.quickChecks(spark, dict))
        assert(sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = true, "StringLengthCheck on column 'item'", 4, 2)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "Item1") :: Nil,
              "Item1",
              "StringLengthCheck failed! item = Item1 and (length('item) > 2)"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "Item23") :: Nil,
              "Item23",
              "StringLengthCheck failed! item = Item23 and (length('item) > 2)"
            )
        )
      }

      it("String length check passes for minLength and  maxLength specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(0)), Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)
        ) // scalastyle:ignore
      }

      it("String length check passes for only minLength specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", Some(Json.fromInt(0)), None, None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)
        ) // scalastyle:ignore
      }

      it("String length check passes for only maxLength specified") {
        val dict = new VarSubstitution
        val df = mkDataFrame(spark, defData)
        val sut = StringLengthCheck("item", None, Some(Json.fromInt(6)), None) // scalastyle:ignore
        val config = ValidatorConfig(
          1,
          2,
          None,
          detailedErrors = true,
          None,
          None,
          List(ValidatorDataFrame(df, None, None, sut :: Nil))
        )
        assert(!config.configCheck(spark, dict))
        assert(!config.quickChecks(spark, dict))
        assert(!sut.failed)
        assert(
          sut.getEvents contains
            ValidatorCheckEvent(failure = false, "StringLengthCheck on column 'item'", 4, 0)
        ) // scalastyle:ignore
      }

      it("toJson works") {
        val minLength = Random.nextInt(1000) // scalastyle:ignore
        val minJson = Json.fromInt(minLength)
        val maxJson = Json.fromInt(minLength + Random.nextInt(1000) + 1) // scalastyle:ignore
        val sut = StringLengthCheck("item", Some(minJson), Some(maxJson), None) // scalastyle:ignore
        assert(
          sut.toJson == Json.obj(
            ("type", Json.fromString("stringLengthCheck")),
            ("column", Json.fromString("item")),
            ("minLength", minJson),
            ("maxLength", maxJson),
            ("events", Json.arr())
          )
        )
      }
    }
  }
}
