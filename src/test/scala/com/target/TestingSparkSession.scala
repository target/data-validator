package com.target

import java.util.Properties

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.scalatest._

trait TestingSparkSession extends BeforeAndAfterAll { self: Suite =>

  lazy val spark: SparkSession = TestingSparkSession.sparkSingleton
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext

}

object TestingSparkSession {

  /**
    * config a log4j properties used for testsuite.
    * Copied from org.apache.spark.utils.Util because it private.
    */
  def configTestLog4j(levelOther: String, levelMe: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$levelOther, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n") // scalastyle:ignore regex
    pro.put(s"log4j.logger.${this.getClass.getPackage.getName}", levelMe)
    PropertyConfigurator.configure(pro)
  }

  lazy val sparkSingleton: SparkSession = {
    configTestLog4j("OFF", "OFF")
    SparkSession
      .builder()
      .config("spark.executor.memory", "512mb")
      .config("spark.ui.showConsoleProgress", value = false)
      .master("local[2]")
      .getOrCreate()
  }

}
