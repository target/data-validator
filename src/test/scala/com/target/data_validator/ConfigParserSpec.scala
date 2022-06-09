package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.validator.{MinNumRows, NullCheck}
import io.circe.Json
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfterAll

import scala.io.Source

class ConfigParserSpec extends AnyFunSpec with BeforeAndAfterAll {

  // Silence is golden!
  override def beforeAll(): Unit = TestingSparkSession.configTestLog4j("OFF", "OFF")

  val expectedConfiguration = ValidatorConfig(
    2,
    742, // scalastyle:ignore magic.number
    Some(EmailConfig("smtpHost", "subject", "from", List("to"))),
    detailedErrors = true,
    Some(List(NameValue("foo", Json.fromString("bar")))),
    Some(
      List[ValidatorOutput](
        FileOutput("/user/home/sample.json", None),
        PipeOutput("/apps/dv2kafka.py", Some(true))
      )
    ),
    List(
      ValidatorHiveTable(
        "foo",
        "bar",
        Some(List("one", "two")),
        None,
        List(MinNumRows(Json.fromInt(10294)), NullCheck("mdse_item_i", None)) // scalastyle:ignore magic.number
      ),
      ValidatorOrcFile("LocalFile.orc", None, Some("foo < 10"), List(NullCheck("start_d", None))),
      ValidatorParquetFile("LocFile.parquet", None, Some("bar < 10"), List(NullCheck("end_d", None))),
      ValidatorSpecifiedFormatLoader(
        format = "llama",
        keyColumns = None,
        condition = None,
        checks = List(NullCheck("start_d", None)),
        options = Some(Map("key" -> "value")),
        loadData = Some(List("data.llama"))
      )
    )
  )

  describe("ConfigParser") {

    describe("parse") {

      it("should correctly parse simple yaml config") {
        val config = ConfigParser.parse("""
          | numKeyCols: 2
          | numErrorsToReport: 742
          | email:
          |   smtpHost: smtpHost
          |   subject: subject
          |   from: from
          |   to:
          |    - to
          | detailedErrors: true
          | vars:
          |   - name: foo
          |     value: bar
          |
          | outputs:
          |   - filename: /user/home/sample.json
          |
          |   - pipe: /apps/dv2kafka.py
          |     ignoreError: true
          | tables:
          |   - db: foo
          |     table: bar
          |     keyColumns:
          |       - one
          |       - two
          |     checks:
          |       - type: rowCount
          |         minNumRows: 10294
          |       - type: nullCheck
          |         column: mdse_item_i
          |   - orcFile: LocalFile.orc
          |     condition: "foo < 10"
          |     checks:
          |       - type: nullCheck
          |         column: start_d
          |   - parquetFile: LocFile.parquet
          |     condition: "bar < 10"
          |     checks:
          |       - type: nullCheck
          |         column: end_d
          |   - format: llama
          |     checks:
          |       - type: nullCheck
          |         column: start_d
          |     options:
          |       key: value
          |     loadData:
          |       - data.llama
          """.stripMargin)

        assert(config == Right(expectedConfiguration))
      }

    }

    describe("parseFile") {

      it("should support loading config files by path") {
        val path = getClass.getResource("/test_config.yaml").getPath
        val output = ConfigParser.parseFile(path, Map.empty)
        assert(output == Right(expectedConfiguration))
      }

      it("should support classpath configuration loading with the prefix 'classpath:'") {
        val output = ConfigParser.parseFile("classpath:/test_config.yaml", Map.empty)
        assert(output == Right(expectedConfiguration))
      }

      it("should not confuse classpath and non classpath file loading") {
        val paths = Seq("classpath:src/test/resources/test_config.yaml", "test_config.yaml")

        paths.foreach { path =>
          val output = ConfigParser.parseFile(path, Map.empty)
          assert(output.isLeft)
        }
      }

    }

  }

}
