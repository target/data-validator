package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.JsonUtils._
import io.circe.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class JsonUtilsSpec extends FunSpec with Matchers with TestingSparkSession {
  val TEST_STRING_LENGTH = 10

  describe("JsonUtils") {

    describe("string2Json") {

      it("Simple Int into Json") {
        val randInt = Random.nextInt
        assert(string2Json(s"$randInt") == Json.fromInt(randInt))
      }

      it("String into Json") {
        val randString = Random.nextString(TEST_STRING_LENGTH)
        assert(string2Json(randString) == Json.fromString(randString))
      }

      it("garbage doesn't crash") {
        val garbageString = "{]2389s0fj2}"
        assert(string2Json(garbageString) == Json.fromString(garbageString))
      }

    }

    describe("debugJson") {

      it("Int") {
        val randInt = Random.nextInt()
        assert(debugJson(Json.fromInt(randInt)) == s"Json NUM: $randInt")
      }

      it("Double") {
        val randDouble = Random.nextDouble()
        assert(debugJson(Json.fromDoubleOrNull(randDouble)) == s"Json NUM: $randDouble")
      }

      it("String") {
        val randString = Random.nextString(TEST_STRING_LENGTH)
        assert(debugJson(Json.fromString(randString)) == s"Json STRING: $randString")
      }

      it("Boolean") {
        val randBool = Random.nextBoolean()
        assert(debugJson(Json.fromBoolean(randBool)) == s"Json BOOLEAN: $randBool")

      }

      it("Array") {
        val randArray = Range(0, TEST_STRING_LENGTH).map(_ => Json.fromInt(Random.nextInt))
        assert(debugJson(Json.fromValues(randArray)) contains "Json ARR:")
      }

      it("Null") {
        assert(debugJson(Json.Null) == "Json NULL")
      }

    }

    describe("row2Json") {

      val TEST_STRING = Random.nextString(TEST_STRING_LENGTH)
      val TEST_LONG = Random.nextLong
      val TEST_INT = Random.nextInt
      val TEST_BOOLEAN = Random.nextBoolean
      val TEST_DOUBLE = Random.nextDouble

      val schema = StructType(
        List(
          StructField("string", StringType),
          StructField("long", LongType),
          StructField("int", IntegerType),
          StructField("null", NullType),
          StructField("bool", BooleanType),
          StructField("double", DoubleType)
        )
      )

      val sampleData =
        List(Row(TEST_STRING, TEST_LONG, TEST_INT, null, TEST_BOOLEAN, TEST_DOUBLE)) // scalastyle:ignore

      def mkRow: Row = spark.createDataFrame(sc.parallelize(sampleData), schema).head()

      it("Row with String") {
        val sut = mkRow
        assert(row2Json(sut, 0) == Json.fromString(TEST_STRING))
      }

      it("Row with long") {
        val sut = mkRow
        assert(row2Json(sut, 1) == Json.fromLong(TEST_LONG))
      }

      it("Row with int") {
        val sut = mkRow
        assert(row2Json(sut, 2) == Json.fromInt(TEST_INT))
      }

      it("Row with null") {
        val sut = mkRow
        assert(row2Json(sut, 3) == Json.Null)
      }

      it("Row with bool") {
        val sut = mkRow
        assert(row2Json(sut, 4) == Json.fromBoolean(TEST_BOOLEAN)) // scalastyle:ignore
      }

      it("Row with double") {
        val sut = mkRow
        assert(row2Json(sut, 5) == Json.fromDoubleOrNull(TEST_DOUBLE)) // scalastyle:ignore
      }

      it("Full Row") {
        val sut = mkRow
        assert(
          row2Json(sut) == Json.obj(
            ("string", Json.fromString(TEST_STRING)),
            ("long", Json.fromLong(TEST_LONG)),
            ("int", Json.fromInt(TEST_INT)),
            ("null", Json.Null),
            ("bool", Json.fromBoolean(TEST_BOOLEAN)),
            ("double", Json.fromDoubleOrNull(TEST_DOUBLE))
          )
        )
      }

    }

  }

}
