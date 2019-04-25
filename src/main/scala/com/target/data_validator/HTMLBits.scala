package com.target.data_validator

import scalatags.Text.all._

/**
  * Place for various HTMLBits that are used in generating HTML report.
  */
object HTMLBits {
  def pass: Tag = span(backgroundColor:= "mediumseagreen")("PASS")
  def fail: Tag = span(backgroundColor:= "tomato")("FAIL")

  def status(failed: Boolean): Tag = if (failed) { fail } else { pass }
}
