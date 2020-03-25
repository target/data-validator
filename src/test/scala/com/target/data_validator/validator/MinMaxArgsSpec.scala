package com.target.data_validator.validator

import com.target.TestingSparkSession
import com.target.data_validator.{ValidatorError, VarSubstitution}
import io.circe.Json
import org.apache.spark.sql.DataFrame
import org.scalatest.{FunSpec, Matchers, WordSpec}

class MinMaxArgsSpec extends WordSpec with Matchers with TestingSparkSession {

  "MinMaxArgs" should {
    "not add an error event" when {
      "checking presence of values" when {
        "it is passed only a minValue" in {
          val sut = new MinMaxTestValidator(minValue = Option(Json.fromInt(3)))
          sut.checkValuesPresent()
          sut.getEvents shouldBe empty
        }
        "it is passed only a maxValue" in {
          val sut = new MinMaxTestValidator(maxValue = Option(Json.fromInt(3)))
          sut.checkValuesPresent()
          sut.getEvents shouldBe empty
        }
        "it is passed both a minValue and maxValue, min < max" in {
          val sut = new MinMaxTestValidator(
            minValue = Option(Json.fromInt(2)),
            maxValue = Option(Json.fromInt(3)))
          sut.checkValuesPresent()
          sut.getEvents shouldBe empty
        }
      }
      "checking inclusive flag" when {
        "inclusive is omitted" in {
          val sut = new MinMaxTestValidator(minValue = Option(Json.fromInt(2)))
          sut.checkInclusive()
          sut.getEvents shouldBe empty
        }
        "inclusive is set to a boolean" in {
          val sut = new MinMaxTestValidator(
            minValue = Option(Json.fromInt(2)),
            inclusive = Option(Json.fromBoolean(true)))
          sut.checkInclusive()
          sut.getEvents shouldBe empty
        }
      }
      "checking min < max" when {
        "only min is specified" when {
          "it is a number" in {
            val sut = new MinMaxTestValidator(minValue = Option(Json.fromInt(3)))
            sut.checkMinLessThanMax()
            sut.getEvents shouldBe empty
          }
          "it is a string" in {
            val sut = new MinMaxTestValidator(minValue = Option(Json.fromString("3")))
            sut.checkMinLessThanMax()
            sut.getEvents shouldBe empty
          }
        }
        "only max is specified" when {
          "it is a number" in {
            val sut = new MinMaxTestValidator(maxValue = Option(Json.fromInt(3)))
            sut.checkMinLessThanMax()
            sut.getEvents shouldBe empty
          }
          "it is a string" in {
            val sut = new MinMaxTestValidator(maxValue = Option(Json.fromString("3")))
            sut.checkMinLessThanMax()
            sut.getEvents shouldBe empty
          }
        }
        "min and max are both specified" when {
          "they are numbers" when {
            "min < max" in {
              val sut = new MinMaxTestValidator(
                minValue = Option(Json.fromInt(1)),
                maxValue = Option(Json.fromInt(2)))
              sut.checkMinLessThanMax()
              sut.getEvents shouldBe empty
            }
          }
          "they are strings" when {
            "min != max" in {
              val sut = new MinMaxTestValidator(
                minValue = Option(Json.fromString("2")),
                maxValue = Option(Json.fromString("3")))
              sut.checkMinLessThanMax()
              sut.getEvents shouldBe empty
            }
          }
        }
      }
    }

    "add an error event" when {
      "checking presence of values" when {
        "values are None" in {
          val sut = new MinMaxTestValidator()
          sut.checkValuesPresent()
          sut.getEvents shouldNot be(empty)
        }
      }
      "checking inclusive flag" when {
        "inclusive is set, but it's not boolean" in {
          val sut = new MinMaxTestValidator(inclusive = Option(Json.fromInt(3)))
          sut.checkInclusive()
          sut.getEvents shouldNot be(empty)
        }
      }
      "checking that minimum is less than maximum" when {
        "min and max both specified and are numerical" when {
          "min = max" in {
            val sut = new MinMaxTestValidator(
              minValue = Option(Json.fromInt(3)),
              maxValue = Option(Json.fromInt(3)))
            sut.checkMinLessThanMax()
            sut.getEvents shouldNot be(empty)
          }
          "min > max" in {
            val sut = new MinMaxTestValidator(
              minValue = Option(Json.fromInt(2)),
              maxValue = Option(Json.fromInt(1)))
            sut.checkMinLessThanMax()
            sut.getEvents shouldNot be(empty)
          }
        }
        "min and max both specified and are strings" when {
          "min = max" in {
            val sut = new MinMaxTestValidator(
              minValue = Option(Json.fromString("3")),
              maxValue = Option(Json.fromString("3")))
            sut.checkMinLessThanMax()
            sut.getEvents shouldNot be(empty)
          }
        }
        "min and max are not type-aligned" when {
          "min is a string and max is a number" in {
            val sut = new MinMaxTestValidator(
              minValue = Option(Json.fromInt(3)),
              maxValue = Option(Json.fromString("3")))
            sut.checkMinLessThanMax()
            sut.getEvents shouldNot be(empty)
            sut.getEvents.head.asInstanceOf[ValidatorError].msg should startWith("Unsupported type")
          }
        }
      }
    }
  }

  class MinMaxTestValidator(
                             val minValue: Option[Json] = None,
                             val maxValue: Option[Json] = None,
                             val inclusive: Option[Json] = None)
    extends ValidatorBase with MinMaxArgs {
    override def substituteVariables(dict: VarSubstitution): ValidatorBase = ???
    override def configCheck(df: DataFrame): Boolean = ???
    override def toJson: Json = ???
  }

}
