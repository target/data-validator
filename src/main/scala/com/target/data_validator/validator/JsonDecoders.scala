package com.target.data_validator.validator

import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.auto._

object JsonDecoders extends LazyLogging {

  implicit val decodeChecks: Decoder[ValidatorBase] = new Decoder[ValidatorBase] {
    // FIXME: specifying this Function here instead of Decoder[ValidatorBase] is a smell that these checks
    //        ought to have proper decoder objects instead of a method.
    //        I.e., we're not using the Circe Decoder API as intended.
    private lazy val decoders = Map[String, HCursor => Either[DecodingFailure, ValidatorBase]](
     "rowCount" -> { _.as[MinNumRows] },
     "nullCheck" -> NullCheck.fromJson,
     "negativeCheck" -> NegativeCheck.fromJson,
     "columnMaxCheck" -> { _.as[ColumnMaxCheck] },
     "rangeCheck" -> RangeCheck.fromJson,
     "uniqueCheck" -> UniqueCheck.fromJson,
     "stringLengthCheck" -> StringLengthCheck.fromJson,
     "stringRegexCheck" -> StringRegexCheck.fromJson,
     "sumOfNumericColumnCheck" -> SumOfNumericColumnCheck.fromJson
    )

    final def apply(c: HCursor): Decoder.Result[ValidatorBase] = c.downField("type").as[String].flatMap(getDecoder(c))

    private def getDecoder(cursor: HCursor)(checkType: String) = {
      decoders
        .get(checkType)
        .map(_(cursor))
      match {
        case Some(x) => x
        case None =>
          logger.error(s"Unknown Check `$checkType` in config! Choose one of ${decoders.keys.mkString(",")}.")
          throw new RuntimeException(s"Unknown Check in config `$checkType`")
      }
    }
  }
}
