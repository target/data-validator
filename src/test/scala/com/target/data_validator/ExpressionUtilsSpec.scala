package com.target.data_validator

import ExpressionUtils.orFromList
import com.target.data_validator.validator.ValidatorBase._
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Or}
import org.scalatest._

class ExpressionUtilsSpec extends FunSpec with Matchers {

  describe("ExpressionUtils") {

    describe("orFromList()") {

      val expr1 = GreaterThan(L0, L1)

      it ("Simpler case 1 expression") {
        assert(orFromList(expr1::Nil) == expr1)
      }

      it ("Standard case 2 expressions") {
        assert(orFromList(expr1::expr1::Nil) == Or(expr1, expr1))
      }

      it ("More then 2 case") {
        assert(orFromList(expr1::expr1::expr1::Nil) == Or(expr1, Or(expr1, expr1)))
      }

      it ("Failure case, empty list.") {
        assertThrows[java.lang.IllegalArgumentException](orFromList(Nil))
      }

    }

  }

}
