package com.target.data_validator

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class VarSubstitutionSpec extends AnyFunSpec with Matchers {

  describe("VarSubstitution") {

    it("adding var twice fails") {
      val dict = new VarSubstitution()
      assert(!dict.addString("foo", "bar"))
      assert(dict.addString("foo", "baz"))
      assert(dict.replaceVars("$foo") == Left("bar"))
    }

    it("adding invalid variable fails") {
      val dict = new VarSubstitution()
      assert(dict.addString("99", "99"))
    }

    it("simple var substitution works") {
      val dict = new VarSubstitution
      assert(!dict.addString("animal", "fox"))
      assert(dict.replaceVars("The quick brown $animal.") == Left("The quick brown fox."))
    }

    it("simple var substitution works for scala type variables") {
      val dict = new VarSubstitution
      assert(!dict.addString("animal", "fox"))
      assert(dict.replaceVars("The quick brown ${animal}.") == Left("The quick brown fox."))
    }

    it("missing var produces error") {
      val dict = new VarSubstitution
      assert(dict.replaceVars("The quick $color fox.") ==
        Right(ValidatorError("VariableSubstitution: Can't find values for the following keys, color")))
    }

    it("missing scala var produces error") {
      val dict = new VarSubstitution
      assert(dict.replaceVars("The quick ${color} fox.") ==
        Right(ValidatorError("VariableSubstitution: Can't find values for the following keys, color")))
    }

    it("adding map works") {
      val dict = new VarSubstitution
      dict.addMap(Map[String, String]("one"-> "1", "two"-> "2"))
      assert(dict.dict.size == 2)
      assert(dict.replaceVars("$one, $two") == Left("1, 2"))
    }

    it("short 1 char variables") {
      val dict = new VarSubstitution
      dict.addString("f", "foo")
      assert(dict.replaceVars("${f}|$f") == Left("foo|foo")) // scalastyle:ignore
    }

    describe("VarSubstitution.replaceAll") {

      it("single replacement") {
        assert(VarSubstitution.replaceAll("This is a test.", " a ", " not a ") == "This is not a test.")
      }

      it("multiple replacements") {
        assert(VarSubstitution.replaceAll("$o $o $o", "$o", "one") == "one one one")
      }

      it("no replacement") {
        val str = "String with nothing to replace."
        assert(VarSubstitution.replaceAll(str, "xx", "yy") == str)
      }

    }

    describe("VarSubstitution.getVarName") {

      it("normal var") {
        assert(VarSubstitution.getVarName("$foo").contains("foo"))
      }

      it("scala type var") {
        assert(VarSubstitution.getVarName("${foo}").contains("foo"))
      }

      it("bad scala type variable") {
        assert(VarSubstitution.getVarName("${foo").isEmpty)
      }

    }

    describe("VarSubstitution findVars") {

      it("approves of simple var") {
        assert(VarSubstitution.findVars("$one, $two, $three") == Set("$one", "$two", "$three"))
      }

      it("approves of scala vars") {
        assert(VarSubstitution.findVars("${one}, ${two}, ${three}") == Set("${one}", "${two}", "${three}"))
      }

      it("does find bad vars") {
        assert(VarSubstitution.findVars("$11, $6nop, ${junk") == Set.empty)
      }

    }

    describe("VarSubstitution isVar") {

      it("simple var") {
        assert(VarSubstitution.isVariable("$foo"))
      }

      it("scala var") {
        assert(VarSubstitution.isVariable("${foo}"))
      }

      it("not a var") {
        assert(!VarSubstitution.isVariable("foo"))
      }

    }

  }

}
