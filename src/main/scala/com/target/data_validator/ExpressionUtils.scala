package com.target.data_validator

import org.apache.spark.sql.catalyst.expressions.{Expression, Or}

object ExpressionUtils {
  /**
    * Takes a List[Expression] and joins them together into on big Or() expression.
    * @param exprs - Non Empty List of Expressions.
    * @return Or of all Expressions.
    * throws IllegalArgumentException if exprs is empty.
    */
  @throws[IllegalArgumentException]
  def orFromList(exprs: List[Expression]): Expression = exprs match {
    case exp :: Nil => exp
    case lhs :: rhs :: Nil => Or(lhs, rhs)
    case lhs :: rhs :: rest => rest.foldRight(Or(lhs, rhs))(Or(_, _))
    case Nil => throw new IllegalArgumentException("exprs must be nonEmpty")
  }
}
