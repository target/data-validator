package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.validator.{ColumnMaxCheck, MinNumRows, NegativeCheck, NullCheck}
import io.circe.Json
import org.scalatest.{FunSpec, Matchers}

class ConfigVarSubSpec extends FunSpec with Matchers with TestingSparkSession {

  val baseMap: Map[String, String] =
    Map("one" -> "1", "two" -> "2", "three" -> "3", "four" -> "4", "five" -> "5", "six" -> "6")

  val dict: VarSubstitution = {
    val d = new VarSubstitution
    d.addMap(baseMap)
    d
  }

  describe("ConfigVariable Substitutions") {

    describe("ValidatorTable children") {

      it("ValidatorHiveTable var substitution should work") {
        val sut = ValidatorHiveTable(
          "database$one",
          "table$two",
          Some(List("Col$three", "Col$four")),
          Some("$five == $six"),
          List.empty
        )
        assert(
          sut.substituteVariables(dict) ==
            ValidatorHiveTable("database1", "table2", Some(List("Col3", "Col4")), Some("5 == 6"), List.empty)
        )
      }

      it("Validator OrcFile substitution should work") {
        val sut = ValidatorOrcFile(
          "/$one/$two/orcFile",
          Some(List("Col$three", "Col$four")),
          Some("$five == $six"),
          List.empty
        )
        assert(
          sut.substituteVariables(dict) ==
            ValidatorOrcFile("/1/2/orcFile", Some(List("Col3", "Col4")), Some("5 == 6"), List.empty)
        )
      }

      it("ValidatorDataFrame substitution should work") {
        val df = spark.emptyDataFrame
        val sut = ValidatorDataFrame(df, Some(List("Col$three", "Col$four")), Some("$five == $six"), List.empty)
        assert(
          sut.substituteVariables(dict) ==
            ValidatorDataFrame(df, Some(List("Col3", "Col4")), Some("5 == 6"), List.empty)
        )
      }

    }

    describe("ValidatorBase children") {

      describe("ColumnBased children") {

        describe("MinNumRows") {

          it("should substitute variables properly") {
            val sut = MinNumRows(Json.fromString("$one"))
            assert(sut.substituteVariables(dict) == MinNumRows(Json.fromInt(1)))
          }

        }

        describe("ColumnMaxCheck") {

          it("should substitute variables properly") {
            val sut = ColumnMaxCheck("Col$six", Json.fromString("$five"))
            val newColMaxCheck = sut.substituteVariables(dict).asInstanceOf[ColumnMaxCheck]
            assert(newColMaxCheck.column == "Col6")
            assert(newColMaxCheck.value == Json.fromInt(5)) // scalastyle:ignore
            assert(sut.substituteVariables(dict) == ColumnMaxCheck("Col6", Json.fromInt(5))) // scalastyle:ignore
            assert(!sut.failed)
          }

          it("should fail on bad variables") {
            val check = ColumnMaxCheck("Col$six", Json.fromString("$fivefour"))
            val sut = check.substituteVariables(dict)
            assert(sut.failed)
          }

        }

      }

      describe("RowBased children") {

        describe("NegativeCheck") {

          it("NegativeCheck") {
            val sut = NegativeCheck("Col$four", None)
            assert(sut.substituteVariables(dict) == NegativeCheck("Col4", None))
          }

          it("NegativeCheck bad variable substitution should fail") {
            val check = NegativeCheck("Col$fourfour", None)
            val sut = check.substituteVariables(dict)
            assert(sut.failed)
          }

        }

        describe("NullCheck") {

          it("should substitute variables properly") {
            val sut = NullCheck("Col${one}", None)
            assert(sut.substituteVariables(dict) == NullCheck("Col1", None))
          }

          it("bad variable substitution should fail") {
            val check = NullCheck("Col${unknown}", None)
            val sut = check.substituteVariables(dict)
            assert(sut.failed)
          }

        }

      }

    }

  }

}
