package com.target.data_validator

import cats.syntax.either._
import cats.syntax.functor._
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.Source
import scala.util.{Failure, Success, Try}

object ConfigParser extends LazyLogging {

  // IntelliJ Says this import isn't needed, but it won't compile without it.
  import validator.JsonDecoders._
  import ConfigVar._

  implicit val decodeTable: Decoder[ValidatorTable] =
    List[Decoder[ValidatorTable]](
      Decoder[ValidatorHiveTable].widen,
      Decoder[ValidatorOrcFile].widen,
      Decoder[ValidatorParquetFile].widen
    ).reduceLeft(_ or _)

  def configFromJson(json: Json): Either[DecodingFailure, ValidatorConfig] = {
    logger.debug(s"Json config: $json")
    json.as[ValidatorConfig]
  }

  def parseFile(filename: String, cliMap: Map[String, String]): Either[Error, ValidatorConfig] = {
    logger.info(s"Parsing `$filename`")
    Try(Source.fromFile(filename).mkString) match {
      case Success(contents) => parse(contents)
      case Failure(thr) => Left[Error, ValidatorConfig](DecodingFailure.fromThrowable(thr, List.empty))
    }
  }

  def parse(conf: String): Either[Error, ValidatorConfig] = parser.parse(conf).flatMap(configFromJson)

  def main(args: Array[String]): Unit = {
    logger.info(s"Args[${args.length}]: $args")
    val filename = args(0)
    var error = false

    parseFile(filename, Map.empty) match {
      case Left(pe) => logger.error(s"Failed to parse $filename, ${pe.getMessage}"); error = true
      case Right(config) => logger.info(s"Config: $config")
    }

    System.exit(if (error) 1 else 0)
  }
}
