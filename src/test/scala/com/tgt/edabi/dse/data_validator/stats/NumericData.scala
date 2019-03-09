package com.tgt.edabi.dse.data_validator.stats

case class NumericData(value1: Double)

object NumericData {

  val data: Seq[NumericData] = Seq(
    NumericData(0.0),
    NumericData(1.0),
    NumericData(2.0),
    NumericData(3.0),
    NumericData(4.0),
    NumericData(5.0),
    NumericData(6.0),
    NumericData(7.0),
    NumericData(8.0),
    NumericData(9.0)
  )

  // scalastyle:off
  val firstPassStats = FirstPassStats(10, 4.5, 0, 9)
  val secondPassStats = SecondPassStats(
    3.0276503540974917,
    Histogram(
      Seq(
        Bin(0.0, 0.9, 1),
        Bin(0.9, 1.8, 1),
        Bin(1.8, 2.7, 1),
        Bin(2.7, 3.6, 1),
        Bin(3.6, 4.5, 1),
        Bin(4.5, 5.4, 1),
        Bin(5.4, 6.3, 1),
        Bin(6.3, 7.2, 1),
        Bin(7.2, 8.1, 1),
        Bin(8.1, 9.0, 1)
      )
    )
  )
  // scalastyle:on

}