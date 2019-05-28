package com.target.data_validator

import cats.syntax.either._
import cats.syntax.functor._
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml.parser

import scala.io.{BufferedSource, Source}
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

  private def bufferContentsAsString(buffer: BufferedSource): String = {
    val contents = buffer.mkString
    buffer.close()
    contents
  }

  private def loadFromFile(filename: String): String = {
    logger.info(s"Attempting to load `$filename` from file system")
    val buffer = Source.fromFile(filename)
    bufferContentsAsString(buffer)
  }

  private def loadFromClasspath(filename: String): String = {
    logger.info(s"Attempting to load `$filename` from classpath")
    val is = getClass.getResourceAsStream(filename)
    val buffer = Source.fromInputStream(is)
    bufferContentsAsString(buffer)
  }

  def parseFile(filename: String, cliMap: Map[String, String]): Either[Error, ValidatorConfig] = {
    logger.info(s"Parsing `$filename`")

    Try {
      if (filename.startsWith("classpath:")) {
        loadFromClasspath(filename.stripPrefix("classpath:"))
      } else {
        loadFromFile(filename)
      }
    } match {
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
