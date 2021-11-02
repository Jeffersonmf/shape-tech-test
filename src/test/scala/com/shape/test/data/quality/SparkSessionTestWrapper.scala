package com.shape.test.data.quality

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession.builder().master("local").appName("spark session")
      .config("spark.some.config.option", true)
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.cores.max", "2")
      .getOrCreate()
  }

}
