package com.target.data_validator

import com.target.TestingSparkSession
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ValidatorOutputSpec extends AnyFunSpec with Matchers with TestingSparkSession {

  val dict = new VarSubstitution()

  describe("ValidatorOutput") {

    describe("PipeOutput") {

      it("variable substitution") {
        dict.addString("TMPDIR", "/tmp")
        val sut = PipeOutput("$TMPDIR/foo.sh", None)
        assert(sut.substituteVariables(dict) == PipeOutput("/tmp/foo.sh", None))
      }

    }

    describe("FileOutput") {

      it("variable substitution") {
        dict.addString("TMPDIR", "/tmp")
        val sut = FileOutput("$TMPDIR/foo.json", None)
        assert(sut.substituteVariables(dict) == FileOutput("/tmp/foo.json", None))
      }

    }

  }

}
