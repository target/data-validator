package com.target.data_validator

import com.target.data_validator.EnvironmentVariables.{Inaccessible, Present, Unset}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EnvironmentVariablesSpec extends AnyWordSpec with Matchers {

  "EnvironmentVariables" should {
    "get envvars" when {
      "an envvar exists" in {
        EnvironmentVariables.get("HOME") should be(Present(System.getenv("HOME")))
      }
      "an envvar doesn't exist" in {
        EnvironmentVariables.get("NOPE") should be(Unset)
      }
      "an envvar isn't an envvar" in {
        EnvironmentVariables.get(null) shouldBe a[Inaccessible] // scalastyle:ignore
      }
    }
    "log envvars" when {
      "using get" in {
        EnvironmentVariables.get("HOME")
        EnvironmentVariables.accessedEnvVars.keySet should contain("HOME")
      }
      "using tryGet" in {
        EnvironmentVariables.tryGet("HOME")
        EnvironmentVariables.accessedEnvVars.keySet should contain("HOME")
      }
    }
  }
}
