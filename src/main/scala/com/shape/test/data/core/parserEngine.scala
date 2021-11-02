package com.shape.test.data.core

import com.shape.test.data.config.environment
import org.apache.spark.rdd.RDD
import java.nio.charset.StandardCharsets

object parserEngine extends SparkSessionWrapper {

  def initializeDataLake() = {
    transformData.equimentSensorsCharger()
    transformData.equipmentCharger()

    val dataFailureLogs = transformData.failuresEquipmentSensorsLoader()
    val treatedFailureLogs = transformData.loadContents(dataFailureLogs)

    transformData.failuresEquipmentSensorsCharger(treatedFailureLogs)
  }

  def loadDataFromParquet() = {




  }
}
