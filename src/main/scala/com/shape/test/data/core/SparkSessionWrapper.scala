package com.shape.test.data.core

import org.apache.spark.sql.SparkSession

/**
 * Interface
 */
trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session")
      .config("spark.some.config.option", true)
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "1g")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.cores.max", "1")
      .getOrCreate()
  }
}
