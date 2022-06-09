package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.TestHelpers.{mkConfig, mkDataFrame, mkDictJson}
import com.target.data_validator.validator.NullCheck
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ValidatorTableSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  val schema = StructType(
    List(StructField("name", StringType), StructField("age", IntegerType), StructField("teamMember", BooleanType))
  )

  // scalastyle:off magic.number
  val doug = Row("Doug", 50, true)
  val collin = Row("Collin", 32, false)

  private val defaultDf = mkDataFrame(List(doug, collin), schema)(spark, sc)

  describe("ValidatorTable") {

    describe("createKeySelect()") {

      it("should return proper keys") {
        val validatorTable = ValidatorDataFrame(defaultDf, Some(List("name")), None, List.empty)
        implicit val config: ValidatorConfig = mkConfig(List(validatorTable))
        assert(validatorTable.createKeySelect(defaultDf) == List("name"))
      }

      it("should return first 2 cols as keySelect") {
        val validatorTable = ValidatorDataFrame(defaultDf, None, None, List.empty)
        implicit val config: ValidatorConfig = mkConfig(List(validatorTable))
        assert(validatorTable.createKeySelect(defaultDf) == List("name", "age"))
      }

    }

    describe("configCheck") {

      it("should detect bad validator column") {
        val dict = new VarSubstitution
        val validatorTable = ValidatorDataFrame(defaultDf, Some(List("Junk")), None, List.empty)
        implicit val config: ValidatorConfig = mkConfig(List(validatorTable))
        assert(validatorTable.configCheck(spark, dict))
      }

    }

    describe("Condition") {

      it("should apply to df") {
        val validatorTable = ValidatorDataFrame(defaultDf, None, Some("age > 40"), List.empty)
        implicit val config: ValidatorConfig = mkConfig(List(validatorTable))
        assert(validatorTable.open(spark).get.collect() === Array(doug))
      }

    }

    describe("variable substitution") {

      describe("should work for all part of HiveTable") {

        it("variable substitution should work for database") {
          val vt = ValidatorHiveTable("$db", "table", None, None, List.empty)
          val dict = mkDictJson(("db", "myDatabase"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(db = "myDatabase"))
          assert(sut.getEvents contains VarSubEvent("$db", "myDatabase"))
        }

        it("variable substitution should work for table") {
          val vt = ValidatorHiveTable("database", "$table", None, None, List.empty)
          val dict = mkDictJson(("table", "myTable"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(table = "myTable"))
          assert(sut.getEvents contains VarSubEvent("$table", "myTable"))
        }

        it("keyColumn") {
          val vt = ValidatorHiveTable("database", "table", Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDictJson(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorHiveTable("database", "table", None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDictJson(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))
        }

        it("checks") {
          val vt = ValidatorHiveTable("database", "table", None, None, List(NullCheck("${nullCol1}", None)))
          val dict = mkDictJson(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorHiveTable]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("${nullCol1}", "nc1"))
        }

      }

      describe("should work for all parts of OrcFile") {

        it("filename") {
          val vt = ValidatorOrcFile("/${env}/path/Data.orc", None, None, List.empty) // scalastyle:ignore
          val dict = mkDictJson(("env", "prod"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(orcFile = "/prod/path/Data.orc"))
          assert(
            sut.getEvents contains VarSubEvent("/${env}/path/Data.orc", "/prod/path/Data.orc") // scalastyle:ignore
          )
        }

        it("keyColumns") {
          val vt = ValidatorOrcFile("OrcFile", Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDictJson(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorOrcFile("OrcFile", None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDictJson(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))
        }

        it("checks") {
          val vt = ValidatorOrcFile("OrcFile", None, None, List(NullCheck("${nullCol1}", None)))
          val dict = mkDictJson(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorOrcFile]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("${nullCol1}", "nc1"))
        }

      }

      describe("should work for all parts of ValidatorDataFrame") {

        it("keyColumns") {
          val vt = ValidatorDataFrame(spark.emptyDataFrame, Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDictJson(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorDataFrame(spark.emptyDataFrame, None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDictJson(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))

        }

        it("checks") {
          val vt = ValidatorDataFrame(spark.emptyDataFrame, None, None, List(NullCheck("$nullCol1", None)))
          val dict = mkDictJson(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorDataFrame]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("$nullCol1", "nc1"))
        }

      }

      describe("should work for the unique parts of ValidatorSpecifiedFormatLoader") {
        val dict = mkDictJson(
          ("format", "foobar"),
          ("loadData", "barfoo"),
          ("optionRep1", "who"),
          ("optionRep2", "what")
        )

        it("format") {
          val vt = ValidatorSpecifiedFormatLoader("${format}", None, None, List.empty) // scalastyle:ignore
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorSpecifiedFormatLoader]

          assert(sut == vt.copy(format = "foobar"))
          assert(sut.getEvents contains VarSubEvent("${format}", "foobar")) // scalastyle:ignore
        }
        it("loadData") {
          val vt = ValidatorSpecifiedFormatLoader(
            "foobar",
            None,
            None,
            List.empty,
            loadData = Some(List("${loadData}"))
          ) // scalastyle:ignore
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorSpecifiedFormatLoader]

          assert(sut == vt.copy(loadData = Option(List("barfoo"))))
          assert(sut.getEvents contains VarSubEvent("${loadData}", "barfoo")) // scalastyle:ignore
        }
        it("options") {
          val vt = ValidatorSpecifiedFormatLoader(
            "foobar",
            None,
            None,
            List.empty,
            options = Some(
              Map(
                "something" -> "${optionRep1}",
                "somethingElse" -> "${optionRep2}"
              )
            )
          )
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorSpecifiedFormatLoader]

          assert(
            sut == vt.copy(options =
              Some(
                Map(
                  "something" -> "who",
                  "somethingElse" -> "what"
                )
              )
            )
          )
          assert(sut.getEvents contains VarSubEvent("${optionRep1}", "who")) // scalastyle:ignore
          assert(sut.getEvents contains VarSubEvent("${optionRep2}", "what")) // scalastyle:ignore
        }
      }
    }
  }
}
