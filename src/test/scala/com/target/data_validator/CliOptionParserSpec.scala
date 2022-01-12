package com.target.data_validator

import org.scalatest.{FunSpec, Matchers}

class CliOptionParserSpec extends FunSpec with Matchers {

  describe("CliOptionParser") {
    describe("parsing") {
      it("does not handle var option values with commas") {
        val args = Array("--vars", "keyA=value1,value2,keyB=value3")
        CliOptionParser.parser.parse(args, CliOptions()) should be(None)
      }
    }
  }
}
