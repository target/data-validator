package com.target.data_validator

import com.target.TestingSparkSession
import com.target.data_validator.validator.{MinNumRows, NullCheck}
import io.circe.Json
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class ConfigParserSpec extends FunSpec with BeforeAndAfterAll {

  // Silence is golden!
  override def beforeAll(): Unit = TestingSparkSession.configTestLog4j("OFF", "OFF")

  describe("ConfigParser") {

    describe("parse") {

      it("should correctly parse simple yaml config") {
        val config = ConfigParser.parse(
          """
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
          """.stripMargin)

        assert(config == Right(
          ValidatorConfig(
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
                List(MinNumRows(10294), NullCheck("mdse_item_i")) // scalastyle:ignore magic.number
              ),
              ValidatorOrcFile("LocalFile.orc", None, Some("foo < 10"), List(NullCheck("start_d"))),
              ValidatorParquetFile("LocFile.parquet", None, Some("bar < 10"), List(NullCheck("end_d")))
            )
          )
        ))
      }

    }

  }

}
