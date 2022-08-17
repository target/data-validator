package com.target.data_validator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class CliOptionParserSpec extends AnyFunSpec with Matchers {

  describe("CliOptionParser") {
    describe("parsing") {
      it("does not handle var option values with commas") {
        val args = Array("--vars", "keyA=value1,value2,keyB=value3")
        CliOptionParser.parser.parse(args, CliOptions()) should be(None)
      }
    }
  }
}
