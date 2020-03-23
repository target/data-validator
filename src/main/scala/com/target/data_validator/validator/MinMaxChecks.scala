package com.target.data_validator.validator

import com.target.data_validator.JsonUtils.debugJson
import com.target.data_validator.ValidatorError
import io.circe.Json

trait MinMaxChecks { this: ValidatorBase =>
  val minValue: Option[Json]
  val maxValue: Option[Json]
  val inclusive: Option[Json]

  lazy val minMaxList: List[Json] = (minValue :: maxValue :: Nil).flatten

  def checkValuesPresent(): Unit = {
    if (minMaxList.isEmpty || minMaxList.size > 2) {
      addEvent(ValidatorError(s"Min or Max or both must be defined"))
    }
  }

  def checkInclusive(): Unit = {
    def errorInclusiveIsNonBoolean(): Unit = {
      val msg = s"Config property 'inclusive' defined but not boolean, is: ${inclusive.get.toString()}"
      logger.error(msg)
      addEvent(ValidatorError(msg))
    }

    if (inclusive.exists(_.asBoolean.isEmpty)) {
      errorInclusiveIsNonBoolean()
    }
  }

  /**
    * Checks that the first element of a list of two numbers is less than the second element.
    */
  def checkMinLessThanMax(): Unit = {
    if (minMaxList.forall(_.isNumber)) {
      minMaxList.flatMap(_.asNumber) match {
        case mv :: xv :: Nil if mv.toDouble >= xv.toDouble =>
          addEvent(ValidatorError(s"Min: $mv must be less than or equal to max: $xv"))
        case _ =>
      }
    } else if (minMaxList.forall(_.isString)) {
      minMaxList.flatMap(_.asString) match {
        case mv :: xv :: Nil if mv == xv =>
          addEvent(ValidatorError(s"Min[String]: $mv must be less than max[String]: $xv"))
        case _ =>
      }
    } else {
      // Not Strings or Numbers
      addEvent(ValidatorError(s"Unsupported type in ${minMaxList.map(debugJson).mkString(", ")}"))
    }
  }
}
