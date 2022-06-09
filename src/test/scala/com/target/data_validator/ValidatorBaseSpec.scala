package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.validator.{MinNumRows, NullCheck, ValidatorBase}
import com.target.data_validator.validator.ValidatorBase._
import io.circe.Json
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import scala.util.Random

class ValidatorBaseSpec extends AnyFunSpec with Matchers with TestingSparkSession {
  val nullCheck = List(NullCheck("name", None))
  val nameStructField = StructField("name", StringType)
  val schema = StructType(List(nameStructField, StructField("age", IntegerType)))

  def mkConfig(df: DataFrame, validators: List[ValidatorBase]): ValidatorConfig =
    ValidatorConfig(
      3,
      10, // scalastyle:ignore
      None,
      detailedErrors = false,
      None,
      None,
      List(ValidatorDataFrame(df, None, None, validators))
    )

  def mkDataFrame(spark: SparkSession, data: List[Row] = Nil): DataFrame =
    spark.createDataFrame(sc.parallelize(data), schema)

  describe("ValidatorBase") {

    describe("isColumnInDataFrame()") {

      it("returns true is column is found in DataFrame") {
        val sut = mkDataFrame(spark)
        assert(isColumnInDataFrame(sut, "name"))
      }

      it("returns false if column can't be found") {
        val sut = mkDataFrame(spark)
        assert(!isColumnInDataFrame(sut, "badCol"))
      }
    }

    describe("areTypesCompatible()") {

      it("StringType is compatible with Json String") {
        assert(areTypesCompatible(StringType, Json.fromString("string")))
      }

      it("StringType is compatible with Json Number") {
        assert(areTypesCompatible(StringType, Json.fromInt(Random.nextInt)))
      }

      it("IntType is not compatible with Json String") {
        assert(!areTypesCompatible(IntegerType, Json.fromString("foo")))
      }

      it("IntType is not compatible with Int.MaxValue + 10") {
        assert(!areTypesCompatible(IntegerType, Json.fromLong(Int.MaxValue.asInstanceOf[Long] + 10L)))
      }

    }

    describe("areNumberTypesCompatible()") {

      it("LongType is compatible with Long") {
        assert(areNumberTypesCompatible(LongType, Json.fromLong(Random.nextLong()).asNumber))
      }

      it("ByteType is not compatible with Long") {
        assert(!areNumberTypesCompatible(ByteType, Json.fromLong(Byte.MaxValue + 1).asNumber))
      }

    }

    describe("isValueColumn()") {

      it("detects column specified in value") {
        assert(isValueColumn("`price"))
      }

      it("detects non-column specified in value") {
        assert(!isValueColumn("2018-12-25"))
      }

      it("detects column specified in json value") {
        assert(isValueColumn(Json.fromString("`price")))
      }

    }

    describe("lookupValueColumn()") {

      it("finds column") {
        assert(lookupValueColumn(schema, "`name") contains nameStructField)
      }

      it("doesn't find column") {
        assert(lookupValueColumn(schema, "`junk").isEmpty)
      }

    }

    describe("schemaContainsValueColumn()") {

      it("find column") {
        assert(schemaContainsValueColumn(schema, "`name"))
      }

      it("doesn't find column") {
        assert(!schemaContainsValueColumn(schema, "`junk"))
      }
    }

    describe("createLiteralOrUnresolvedAttribute()") {

      it("creates column reference") {
        assert(
          createLiteralOrUnresolvedAttribute(StringType, Json.fromString("`name")) == UnresolvedAttribute("name")
        )
      }

      it("creates String Literal") {
        val eggs = "eggs"
        assert(
          createLiteralOrUnresolvedAttribute(StringType, Json.fromString(eggs)) == Literal.create(eggs, StringType)
        )
      }

      it("creates Numeric Literal") {
        assert(createLiteralOrUnresolvedAttribute(IntegerType, Json.fromInt(0)) == I0)
      }

    }
  }

  describe("ValidatorNullCheck") {

    it("should not error on non-null data") {
      val dict = new VarSubstitution
      val df =
        spark.createDataFrame(sc.parallelize(List(Row("Doug", 50), Row("Collin", 32))), schema) // scalastyle:ignore
      val sut = mkConfig(df, nullCheck)
      assert(!sut.configCheck(spark, dict), "configCheck should not fail!")
      assert(!sut.quickChecks(spark, dict))
    }

    it("should error on null data") {
      val dict = new VarSubstitution
      val df =
        spark.createDataFrame(sc.parallelize(List(Row("Doug", 50), Row(null, 32))), schema) // scalastyle:ignore
      val config = mkConfig(df, nullCheck)
      assert(config.quickChecks(spark, dict))
      assert(config.failed)
      assert(config.tables.head.failed)
    }

    it("should fail configCheck on unknown column") {
      val dict = new VarSubstitution
      val df =
        spark.createDataFrame(sc.parallelize(List(Row("Doug", 50), Row("Collin", 32))), schema) // scalastyle:ignore
      val config = mkConfig(df, List(NullCheck("unknown_column", None)))
      assert(config.configCheck(spark, dict))
      assert(config.failed)
      assert(config.tables.head.failed)
    }
  }

  describe("ValidatorMinNumRows") {

    val df =
      spark.createDataFrame(sc.parallelize(List(Row("Doug", 50), Row("Collin", 32))), schema) // scalastyle:ignore

    it("configCheck() should fail for minNumRows as non-numeric") {
      val dict = new VarSubstitution
      val config = mkConfig(df, List(MinNumRows(Json.fromString("badinput"))))
      assert(config.configCheck(spark, dict))
      assert(config.failed)
      assert(config.tables.head.failed)
    }

    it("configCheck() should fail for negative minNumRows") {
      val dict = new VarSubstitution
      val config = mkConfig(df, List(MinNumRows(Json.fromLong(-10)))) // scalastyle:ignore magic.number
      assert(config.configCheck(spark, dict))
      assert(config.failed)
      assert(config.tables.head.failed)
    }

    it("quickCheck() should fail when rowCount < minNumRows") {
      val dict = new VarSubstitution
      val minNumRowsCheck = MinNumRows(Json.fromLong(10)) // scalastyle:ignore magic.number
      val config = mkConfig(df, List(minNumRowsCheck))
      assert(config.quickChecks(spark, dict))
      assert(config.failed)
      assert(config.tables.head.failed)
      assert(
        minNumRowsCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = true,
          ListMap("expected" -> "10", "actual" -> "2", "relative_error" -> "80.00%"),
          "MinNumRowsCheck Expected: 10 Actual: 2 Relative Error: 80.00%"
        )
      )
    }

    it("quickCheck() should succeed when rowCount > minNumRows") {
      val dict = new VarSubstitution
      val minNumRowsCheck = MinNumRows(Json.fromInt(1))
      val config = mkConfig(df, List(minNumRowsCheck))
      assert(!config.configCheck(spark, dict))
      assert(!config.quickChecks(spark, dict))
      assert(!config.failed)
      assert(!config.tables.exists(_.failed))
      assert(
        minNumRowsCheck.getEvents contains ColumnBasedValidatorCheckEvent(
          failure = false,
          ListMap("expected" -> "1", "actual" -> "2", "relative_error" -> "0.00%"),
          "MinNumRowsCheck Expected: 1 Actual: 2 Relative Error: 0.00%"
        )
      )
    }

  }

}
