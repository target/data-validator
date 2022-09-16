package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers._
import io.circe.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TestHelpersSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  val data = List(
    "item" -> List("item1", "item2", "item3", null), // scalastyle:ignore
    "price" -> List(1.99, 2.99, 3.99, 0.0),
    "count" -> List(1, 2, 3, 0),
    "instock" -> List(true, false, true, false)
  )

  val expectedSchema = StructType(
    List(
      StructField("item", StringType),
      StructField("price", DoubleType),
      StructField("count", IntegerType),
      StructField("instock", BooleanType)
    )
  )

  describe("parseYml") {
    it("parses simple yml") {
      val sut = parseYaml("""
        |double: 2.01
        |int: 10293
        |string: foo
        |array:
        | - one
        | - two
        | - three
        """.stripMargin)
      assert(
        sut == Json.obj(
          ("double", Json.fromDouble(2.01).get),
          ("int", Json.fromInt(10293)), // scalastyle:ignore
          ("string", Json.fromString("foo")),
          ("array", Json.arr(Seq("one", "two", "three").map(Json.fromString): _*))
        )
      )
    }
  }

  describe("mkDict") {
    it("simple case") {
      val sut = mkDict("key" -> "value")
      assert(sut.dict("key") == Json.fromString("value"))
    }
  }

  describe("guessType") {
    it("double") {
      assert(guessType(1.99) == DoubleType) // scalastyle: ignore
    }

    it("int") {
      assert(guessType(1) == IntegerType) // scalastyle: ignore
    }

    it("string") {
      assert(guessType("string") == StringType)
    }

    it("boolean") {
      assert(guessType(true) == BooleanType)
    }

  }

  describe("mkSchema") {
    it("simple") {
      assert(mkSchema(data: _*) == expectedSchema)
    }
  }

  describe("mkRows") {
    assert(
      mkRows(data: _*) == List(
        Row("item1", 1.99, 1, true),
        Row("item2", 2.99, 2, false),
        Row("item3", 3.99, 3, true),
        Row(null, 0.0, 0, false)
      )
    ) // scalastyle:ignore
  }
  // mkDf

  describe("mkDf") {}

}
