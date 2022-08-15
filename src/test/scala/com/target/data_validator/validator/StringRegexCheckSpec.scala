package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator._
import io.circe.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}

class StringRegexCheckSpec extends FunSpec with Matchers with TestingSparkSession with Mocker {

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
    Row(null, 1.00), // scalastyle:ignore
    Row(null, 2.00) // scalastyle:ignore
  )

  describe("StringRegexCheck") {

    describe("configCheck") {

      it("error if regex is not defined") {
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", None, None)
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Must define a regex."))
        assert(sut.failed)
      }

      it("error if column is not found in df") {
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("bad_column_name", Some(Json.fromString("I%")), None)
        assert(sut.configCheck(df))
        assert(sut.getEvents contains ValidatorError("Column: 'bad_column_name' not found in schema."))
        assert(sut.failed)
      }

      it("error if col type is not String") {
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("baseprice", Some(Json.fromString("I%")), None)
        assert(sut.configCheck(df))
        assert(
          sut.getEvents contains
            ValidatorError("Data type of column 'baseprice' must be String, but was found to be DoubleType")
        )
        assert(sut.failed)
      }
    }

    describe("substitute vars") {

      it("variable column name isn't correct") {
        val sut = StringRegexCheck("$column", Some(Json.fromString("I%")), None)
        assert(sut.substituteVariables(mkParams()) == sut)
        assert(sut.failed)
      }

      it("substitute without threshold") {
        val dict = mkParams(List(("column", "item"), ("regex", "I%")))
        val sut = StringRegexCheck("$column", Some(Json.fromString("${regex}")), None)
        assert(sut.substituteVariables(dict) == StringRegexCheck("item", Some(Json.fromString("I%")), None))
        assert(!sut.failed)
      }

      it("substitute with threshold") {
        val dict =
          mkParams(List(("column", "item"), ("regex", "I%"), ("threshold", Json.fromInt(100)))) // scalastyle:ignore
        val sut = StringRegexCheck("$column", Some(Json.fromString("${regex}")), Some("${threshold}"))
        assert(sut.substituteVariables(dict) == StringRegexCheck("item", Some(Json.fromString("I%")), Some("100")))
        assert(!sut.failed)
      }
    }

    describe("colTest") {

      it("regex pattern ab%") {
        val sut = StringRegexCheck("item", Some(Json.fromString("ab%")), None)
        assert(
          sut.colTest(schema, mkParams()).sql == And(
            Not(RLike(UnresolvedAttribute("item"), Literal.create("ab%", StringType))),
            IsNotNull(UnresolvedAttribute("item"))
          ).sql
        )
      }
    }

    describe("StringRegexCheck.fromJson") {

      it("fromJson with regex and threshold") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringRegexCheck
            |  column: item
            |  regex: ab%
            |  threshold: 10%
              """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringRegexCheck("item", Some(Json.fromString("ab%")), Some("10%")))
      }

      it("fromJson with regex") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringRegexCheck
            |  column: item
            |  regex: ab%
          """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringRegexCheck("item", Some(Json.fromString("ab%")), None))
      }

      it("fromJson with threshold") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringRegexCheck
            |  column: item
            |  threshold: 10%
          """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringRegexCheck("item", None, Some("10%")))
      }

      it("fromJson without regex and threshold") {
        import JsonDecoders.decodeChecks
        val yaml =
          """---
            |- type: stringRegexCheck
            |  column: item
          """.stripMargin
        val json = io.circe.yaml.parser.parse(yaml).right.getOrElse(Json.Null)
        val sut = json.as[Array[ValidatorBase]]
        assert(sut.isRight)
        assert(sut.right.get contains StringRegexCheck("item", None, None))
      }
    }

    describe("Regex Match Check") {

      it("String regex check fails : numFailures=1 : numFailuresToReport=2") {
        val dict = mkParams()
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", Some(Json.fromString("^It")), None) // scalastyle:ignore
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
            ValidatorCheckEvent(failure = true, "StringRegexCheck on column 'item'", 5, 1)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringRegexCheck failed! item = I and (NOT 'item RLIKE ^It && isnotnull('item))"
            )
        )
      }

      it("String regex check fails : numFailures=2 : numFailuresToReport=2") {
        val dict = mkParams()
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", Some(Json.fromString("^Item2")), None) // scalastyle:ignore
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
            ValidatorCheckEvent(failure = true, "StringRegexCheck on column 'item'", 5, 2)
        ) // scalastyle:ignore

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringRegexCheck failed! item = I and (NOT 'item RLIKE ^Item2 && isnotnull('item))"
            )
        )

        assert(
          sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "Item1") :: Nil,
              "Item1",
              "StringRegexCheck failed! item = Item1 and (NOT 'item RLIKE ^Item2 && isnotnull('item))"
            )
        )
      }

      it("String regex check fails : numFailures=2 : numFailuresToReport=1") {
        val dict = mkParams()
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", Some(Json.fromString("^Item2")), None) // scalastyle:ignore
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
            ValidatorCheckEvent(failure = true, "StringRegexCheck on column 'item'", 5, 2)
        ) // scalastyle:ignore

        assert(
          (sut.getEvents contains
            ValidatorQuickCheckError(
              ("item", "I") :: Nil,
              "I",
              "StringRegexCheck failed! item = I and (NOT 'item RLIKE ^Item2 && isnotnull('item))"
            )) ^
            (sut.getEvents contains
              ValidatorQuickCheckError(
                ("item", "Item1") :: Nil,
                "Item1",
                "StringRegexCheck failed! item = Item1 and (NOT 'item RLIKE ^Item2 && isnotnull('item))"
              ))
        )
      }

      it("String regex check passes 1") {
        val dict = mkParams()
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", Some(Json.fromString("^I")), None) // scalastyle:ignore
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
            ValidatorCheckEvent(failure = false, "StringRegexCheck on column 'item'", 5, 0)
        ) // scalastyle:ignore
      }

      it("String regex check passes 2") {
        val dict = mkParams()
        val df = mkDataFrame(spark, defData, schema)
        val sut = StringRegexCheck("item", Some(Json.fromString("\\w")), None) // scalastyle:ignore
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
            ValidatorCheckEvent(failure = false, "StringRegexCheck on column 'item'", 5, 0)
        ) // scalastyle:ignore
      }

    }
  }

}
