package com.target.data_validator

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ListBuffer

trait EventLog extends EventGenerator with LazyLogging {
  def addEvent(ve: ValidatorEvent): Unit = EventLog.events.append(ve)

  def validatorError(msg: String): Unit = {
    logger.error(msg)
    addEvent(ValidatorError(msg))
  }
}

object EventLog extends LazyLogging {
  val events = new ListBuffer[ValidatorEvent]
}
