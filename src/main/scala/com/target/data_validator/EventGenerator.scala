package com.target.data_validator

trait EventGenerator {
  def addEvent(ve: ValidatorEvent): Unit
}
