package com.target.data_validator

import com.hortonworks.hwc.HiveWarehouseSession
import com.target.{data_validator, TestingSparkSession}
import com.target.data_validator.validator.NullCheck
import io.circe.Json
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.{FunSpec, Matchers}

class ValidatorTableSpec extends FunSpec with Matchers with TestingSparkSession {

  val schema = StructType(List(StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("teamMember", BooleanType)))

  def mkConfig(tables: List[ValidatorTable]): ValidatorConfig =
    ValidatorConfig(2, 10, None, detailedErrors = false, None, None, tables) // scalastyle:ignore

  def mkDataFrame(data: List[Row]): DataFrame = spark.createDataFrame(sc.parallelize(data), schema)

  // scalastyle:off magic.number
  val doug = Row("Doug", 50, true)
  val collin = Row("Collin", 32, false)

  private val defaultDf = mkDataFrame(List(doug, collin))

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

      def mkDict(elems: (String, String)*): VarSubstitution = {
        val ret = new VarSubstitution
        elems.foreach(e => ret.add(e._1, JsonUtils.string2Json(e._2)))
        ret
      }

      describe("should work for all part of HiveTable") {

        it("variable substitution should work for useHiveWarehouseConnector") {
          val vt = ValidatorHiveTable(Some(Json.fromString("$useHiveWarehouseConnector")), "database", "table", None, None, List.empty) // scalastyle:ignore
          val dict = mkDict(("useHiveWarehouseConnector", "true"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(useHiveWarehouseConnector = Some(Json.fromBoolean(true))))
          assert(sut.getEvents contains VarSubJsonEvent("\"$useHiveWarehouseConnector\"", Json.fromBoolean(true)))
        }

        it("variable substitution should work for database") {
          val vt = ValidatorHiveTable(None, "$db", "table", None, None, List.empty)
          val dict = mkDict(("db", "myDatabase"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(db = "myDatabase"))
          assert(sut.getEvents contains VarSubEvent("$db", "myDatabase"))
        }

        it("variable substitution should work for table") {
          val vt = ValidatorHiveTable(None, "database", "$table", None, None, List.empty)
          val dict = mkDict(("table", "myTable"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(table = "myTable"))
          assert(sut.getEvents contains VarSubEvent("$table", "myTable"))
        }

        it ("keyColumn") {
          val vt = ValidatorHiveTable(None, "database", "table", Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDict(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorHiveTable(None, "database", "table", None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDict(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))
        }

        it("checks") {
          val vt = ValidatorHiveTable(None, "database", "table", None, None, List(NullCheck("${nullCol1}", None)))
          val dict = mkDict(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorHiveTable]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("${nullCol1}", "nc1"))
        }

      }

      describe("should work for all parts of OrcFile") {

        it("filename") {
          val vt = ValidatorOrcFile("/${env}/path/Data.orc", None, None, List.empty) // scalastyle:ignore
          val dict = mkDict(("env", "prod"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(orcFile = "/prod/path/Data.orc"))
          assert(sut.getEvents contains VarSubEvent("/${env}/path/Data.orc", "/prod/path/Data.orc")) // scalastyle:ignore
        }

        it("keyColumns") {
          val vt = ValidatorOrcFile("OrcFile", Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDict(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorOrcFile("OrcFile", None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDict(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))
        }

        it("checks") {
          val vt = ValidatorOrcFile("OrcFile", None, None, List(NullCheck("${nullCol1}", None)))
          val dict = mkDict(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorOrcFile]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("${nullCol1}", "nc1"))
        }

      }

      describe("should work for all parts of ValidatorDataFrame") {

        it("keyColumns") {
          val vt = data_validator
            .ValidatorDataFrame(spark.emptyDataFrame, Some(List("$key1", "$key2")), None, List.empty)
          val dict = mkDict(("key1", "col1"), ("key2", "col2"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(keyColumns = Some(List("col1", "col2"))))
          assert(sut.getEvents contains VarSubEvent("$key1", "col1"))
          assert(sut.getEvents contains VarSubEvent("$key2", "col2"))
        }

        it("condition") {
          val vt = ValidatorDataFrame(spark.emptyDataFrame, None, Some("end_d < '$end_date'"), List.empty)
          val dict = mkDict(("end_date", "2018-11-26"))
          val sut = vt.substituteVariables(dict)
          assert(sut == vt.copy(condition = Some("end_d < '2018-11-26'")))
          assert(sut.getEvents contains VarSubEvent("end_d < '$end_date'", "end_d < '2018-11-26'"))

        }

        it("checks") {
          val vt = ValidatorDataFrame(spark.emptyDataFrame, None, None, List(NullCheck("$nullCol1", None)))
          val dict = mkDict(("nullCol1", "nc1"), ("nullCol2", "nc2"))
          val sut = vt.substituteVariables(dict).asInstanceOf[ValidatorDataFrame]
          assert(sut == vt.copy(checks = List(NullCheck("nc1", None))))
          assert(sut.checks.head.getEvents contains VarSubEvent("$nullCol1", "nc1"))
        }

      }

    }

    describe("access hive tables") {

      it("should use SparkSession when useHiveWarehouseConnector is not specified") {
        val vt = ValidatorHiveTable(None, "database", "table", None, None, List.empty)
        vt.createConnector(spark) match {
          case Right(hiveWarehouseSession: HiveWarehouseSession) => fail("Expected hive table to be accessed by SparkSession, but found HiveWarehouseSession instead") // scalastyle:ignore
          case Left(session: SparkSession) => assert(true)
        }
      }

      it("should use SparkSession when useHiveWarehouseConnector is set to false") {
        val vt = ValidatorHiveTable(Some(Json.fromBoolean(false)), "database", "table", None, None, List.empty)
        vt.createConnector(spark) match {
          case Right(hiveWarehouseSession: HiveWarehouseSession) => fail("Expected hive table to be accessed by SparkSession, but found HiveWarehouseSession instead") // scalastyle:ignore
          case Left(session: SparkSession) => assert(true)
        }
      }

      it("should use HiveWarehouseSession when useHiveWarehouseConnector is set to true") {
        val vt = ValidatorHiveTable(Some(Json.fromBoolean(true)), "database", "table", None, None, List.empty)
        val sparkSession: SparkSession = SparkSession.builder()
                                                     .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://some.hiveserver2.url:port")
                                                     .getOrCreate()
        vt.createConnector(sparkSession) match {
          case Left(session: SparkSession) => fail("Expected hive table to be accessed by HiveWarehouseSession, but found SparkSession instead") // scalastyle:ignore
          case Right(hiveWarehouseSession: HiveWarehouseSession) => assert(true)
        }
      }

    }

  }

}
