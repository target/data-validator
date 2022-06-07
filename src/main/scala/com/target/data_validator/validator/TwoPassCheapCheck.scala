package com.target.data_validator.validator

import org.apache.spark.sql._

/** extension of [[CheapCheck]] with an assumption that DV will:
  *   - complete a pre-pass stage that generates an intermediary aggregate
  *   - provide that intermediary aggregate so it can be used in generating the final check expression
  */
abstract class TwoPassCheapCheck extends CheapCheck {

  def hasQuickErrorDetails: Boolean = false

  /** defined by implementor, should generate one row of aggregated output that can then be handled by
    * [[sinkFirstPassRow]]
    */
  def firstPassSelect(): Column

  /** defined by implementor, notify the cheap check of the result of the first pass projection
    *
    * NOTE: the contract for this check type assumes you call this method BEFORE [[CheapCheck.select]]
    */
  def sinkFirstPassRow(row: Row): Unit

}
