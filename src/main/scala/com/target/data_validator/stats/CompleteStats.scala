package com.target.data_validator.stats

import io.circe._
import io.circe.generic.semiauto._

case class CompleteStats(
    name: String,
    column: String,
    count: Long,
    mean: Double,
    min: Double,
    max: Double,
    stdDev: Double,
    histogram: Histogram
)

object CompleteStats {
  implicit val binEncoder: Encoder[Bin] = deriveEncoder
  implicit val histogramEncoder: Encoder[Histogram] = deriveEncoder
  implicit val encoder: Encoder[CompleteStats] = deriveEncoder

  implicit val binDecoder: Decoder[Bin] = deriveDecoder
  implicit val histogramDecoder: Decoder[Histogram] = deriveDecoder
  implicit val decoder: Decoder[CompleteStats] = deriveDecoder

  def apply(
      name: String,
      column: String,
      firstPassStats: FirstPassStats,
      secondPassStats: SecondPassStats
  ): CompleteStats = CompleteStats(
    name = name,
    column = column,
    count = firstPassStats.count,
    mean = firstPassStats.mean,
    min = firstPassStats.min,
    max = firstPassStats.max,
    stdDev = secondPassStats.stdDev,
    histogram = secondPassStats.histogram
  )
}
