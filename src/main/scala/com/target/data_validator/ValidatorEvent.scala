package com.target.data_validator

import java.util.concurrent.TimeUnit

import io.circe.Json

import scalatags.Text
import scalatags.Text.all._

trait ValidatorEvent {
  def failed: Boolean
  def toHTML: Tag

  def failedHTML: Tag = HTMLBits.status(failed)
}

case class ValidatorCounter(name: String, value: Long) extends ValidatorEvent {
  override def failed: Boolean = false
  override def toHTML: Tag = {
    div(cls:="counter")(s"Counter - $name: $value")
  }
}

case class ValidatorError(msg: String) extends ValidatorEvent {
  override def failed: Boolean = true

  override def toHTML: Text.all.Tag = div(cls:="error")(failedHTML, msg)
}

case class ValidatorCheckEvent(failure: Boolean, label: String, count: Long, errorCount: Long) extends ValidatorEvent {
  override def failed: Boolean = failure

  override def toHTML: Text.all.Tag = {
    val pct = "%4.2f%%".format((errorCount * 100.0) / count)
    div(cls:="checkEvent")(failedHTML, s" - $label count: $count errors: $errorCount pct: $pct")
  }
}

case class ColumnBasedValidatorCheckEvent(failure: Boolean,
                                          data: Map[String, String],
                                          msg: String) extends ValidatorEvent {
  override def failed: Boolean = failure

  override def toHTML: Text.all.Tag = {
    div(cls:="checkEvent")(failedHTML, s" - $msg")
  }
}

class ValidatorTimer(val label: String) extends ValidatorEvent {
  var duration = 0L

  override def failed: Boolean = false

  def time[R](block: => R): R = {
    val start = System.nanoTime()
    val result = try {
      block
    } finally {
      duration = System.nanoTime() - start
    }
    result
  }

  def toSecs: Long = TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS)

  override def toHTML: Text.all.Tag = div(cls:="timer")(s"Timer: $label took $toSecs seconds.")

  override def toString: String = s"Time: $label Duration: $toSecs seconds"
}

case class ValidatorQuickCheckError(key: List[(String, Any)], value: Any, message: String) extends ValidatorEvent {
  override def failed: Boolean = true
  override def toHTML: Text.all.Tag = div(cls:="quickCheckError")(failedHTML, " - " + toString)

  def keyToString: String = "{" + key.map { case (c, v) => s"$c:$v" }.mkString(", ") + "}"

  override def toString: String = {
    val vStr = Option(value).getOrElse("(NULL)").toString
    s"ValidatorQuickCheckError(key: $keyToString, value: $vStr msg: $message)"
  }
}

case class ValidatorGood(msg: String) extends ValidatorEvent {
  override def failed: Boolean = false
  override def toString: String = msg
  override def toHTML: Text.all.Tag = div(cls:="good")(msg)
}

case class VarSubEvent(src: String, dest: String) extends ValidatorEvent {
  override def failed: Boolean = false
  override def toString: String = s"VarSub src: $src dest: $dest"
  override def toHTML: Text.all.Tag = div(cls:="subEvent")(toString)
}

case class VarSubJsonEvent(src: String, dest: Json) extends ValidatorEvent {
  override def failed: Boolean = false
  override def toString: String = s"VarSub src: $src dest: ${dest.noSpaces}"
  override def toHTML: Text.all.Tag = div(cls:="subEvent")(toString)
}
