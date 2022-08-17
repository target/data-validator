package com.target.data_validator.validator

import cats.syntax.either._
import com.target.TestingSparkSession
import com.target.data_validator._
import com.target.data_validator.ConfigVar._
import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.util.Random
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ConfigVarSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  describe("ConfigVar") {

    describe("NameValue") {

      it("from Json snippet") {
        val json: Json =
          parse("""{ "name": "foo", "value": "bar" }""").getOrElse(Json.Null)
        val sut = json.as[ConfigVar]
        assert(sut == Right(NameValue("foo", Json.fromString("bar"))))
      }

      it("addEntry works") {
        val bar = Json.fromString("bar")
        val sut = NameValue("foo", bar)
        val varSub = new VarSubstitution
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict.get("foo") contains bar)
      }

      it("asJson works") {
        val sut = NameValue("bar", Json.fromString("foo"))
        assert(sut.asJson.noSpaces == """{"name":"bar","value":"foo"}""")
      }

      it("var sub in value") {
        val varSub = new VarSubstitution
        assert(!varSub.addString("four", "4"))
        val sut = NameValue("foo", Json.fromString("${four} score"))
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict.get("foo").contains(Json.fromString("4 score")))
      }

      it("var sub fails when value doesn't exist") {
        val varSub = new VarSubstitution
        val sut = NameValue("foo", Json.fromString("${four} score"))
        assert(sut.addEntry(spark, varSub))
      }

    }

    describe("NameEnv") {

      it("from Json snippet") {
        val json: Json = parse("""{ "name":"foo", "env":"ENV"}""").getOrElse(Json.Null)
        val sut = json.as[ConfigVar]
        assert(sut == Right(NameEnv("foo", "ENV")))
      }

      it("addEntry works") {
        val sut = NameEnv("java_home", "JAVA_HOME")
        val varSub = new VarSubstitution
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict.get("java_home") contains Json.fromString(System.getenv("JAVA_HOME")))
      }

      it("asJson works") {
        val sut = NameEnv("foo", "bar")
        assert(sut.asJson.noSpaces == """{"name":"foo","env":"bar"}""")
      }

      it("var sub in env value") {
        val sut = NameEnv("java_home", "JAVA_${h}")
        val varSub = new VarSubstitution
        assert(!varSub.addString("h", "HOME"))
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict("java_home").asString contains System.getenv("JAVA_HOME"))
      }

      it("var sub fails when value doesn't exist") {
        val sut = NameEnv("java_home", "JAVA_${h}")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
      }

    }

    describe("NameShell") {

      it("from Json snippet") {
        val json: Json = parse("""{ "name":"foo", "shell":"false"}""").getOrElse(Json.Null)
        val sut = json.as[ConfigVar]
        assert(sut == Right(NameShell("foo", "false")))
      }

      it("addEntry works as expected") {
        val sut = NameShell("one", "echo 1")
        val varSub = new VarSubstitution
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict("one") ==  Json.fromInt(1))
      }

      it("asJson works") {
        val sut = NameShell("one", "echo 1")
        assert(sut.asJson.noSpaces == """{"name":"one","shell":"echo 1"}""")
      }

      it("bad command works as expected") {
        val sut = NameShell("one", "/bad/command")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
        assert(EventLog.events exists {
          case ValidatorError(msg) =>
            msg.startsWith("NameShell(one, /bad/command) Ran but returned exitCode: 127 stderr:")
          case _ => false
        })
      }

      it("no output works as expected") {
        val sut = NameShell("one", "true")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
        assert(!varSub.dict.contains("one"))
      }

      it("command failing works as expected") {
        val sut = NameShell("one", "echo 1 && false")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
        assert(!varSub.dict.contains("one"))
        assert(EventLog.events contains
          ValidatorError("NameShell(one, echo 1 && false) Ran but returned exitCode: 1 stderr: "))
      }

      it("variable substitution in command works") {
        val varSub = new VarSubstitution
        val valueJson = Json.fromInt(Random.nextInt)
        varSub.add("one", valueJson)
        val sut = NameShell("one", "echo $one")
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict("one") == valueJson)
      }
    }

    describe("NameSql") {

      it("from Json snippet") {
        val json: Json = parse("""{ "name":"foo", "sql":"select 1"}""").getOrElse(Json.Null)
        val sut = json.as[ConfigVar]
        assert(sut == Right(NameSql("foo", "select 1")))
      }

      it("addEntry works as expected") {
        val sut = NameSql("one", "select 1")
        val varSub = new VarSubstitution
        assert(!sut.addEntry(spark, varSub))
        assert(varSub.dict("one") == Json.fromInt(1))
      }

      it("asJson works") {
        val sut = NameSql("one", "select 1")
        assert(sut.asJson.noSpaces == """{"name":"one","sql":"select 1"}""")
      }

      it("bad sql works as expected") {
        val sut = NameSql("one", "bad sql")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
      }

      it("empty query") {
        val schema = StructType(List(StructField("data", IntegerType)))
        val df = spark.createDataFrame(sc.parallelize(List(Row(10))), schema) // scalastyle:ignore
        df.createTempView("MyTable")
        val sut = NameSql("one", "select data from MyTable where data < 10")
        val varSub = new VarSubstitution
        assert(sut.addEntry(spark, varSub))
      }

    }

  }

}
