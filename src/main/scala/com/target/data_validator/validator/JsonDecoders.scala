package com.target.data_validator.validator

import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Decoder, HCursor}
import io.circe.generic.auto._

object JsonDecoders extends LazyLogging {

  implicit val decodeChecks: Decoder[ValidatorBase] = new Decoder[ValidatorBase] {
    final def apply(c: HCursor): Decoder.Result[ValidatorBase] = c.downField("type").as[String].flatMap {
      case "rowCount" => c.as[MinNumRows]
      case "nullCheck" => NullCheck.fromJson(c)
      case "negativeCheck" => NegativeCheck.fromJson(c)
      case "columnMaxCheck" => c.as[ColumnMaxCheck]
      case "rangeCheck" => RangeCheck.fromJson(c)
      case "uniqueCheck" => UniqueCheck.fromJson(c)
      case "stringLengthCheck" => StringLengthCheck.fromJson(c)
      case x => logger.error(s"Unknown Check `$x` in config!")
        throw new RuntimeException(s"Unknown Check in config `$x`")
    }
  }
}
