package com.shape.test.data.core

import com.shape.test.data.config.environment
import org.apache.spark.sql.functions.{asc, avg, desc, sum}

object parserEngine extends SparkSessionWrapper {

  import org.apache.spark.sql._

  def initializeDataLake() = {
    transformData.equimentSensorsCharger()
    transformData.equipmentCharger()

    val dataFailureLogs = transformData.failuresEquipmentSensorsLoader()
    val treatedFailureLogs = transformData.loadContents(dataFailureLogs)

    transformData.failuresEquipmentSensorsCharger(treatedFailureLogs)
  }

  def loadDataFromParquet(): (DataFrame, DataFrame, DataFrame) = {
    val equipment_failure_sensors = spark.read.parquet(environment.DataLakeLocation(environment.isRunningLocalMode()) + "equipment_failure_sensors")
    val equipments = spark.read.parquet(environment.DataLakeLocation(environment.isRunningLocalMode()) + "equipments")
    val equipment_sensors = spark.read.parquet(environment.DataLakeLocation(environment.isRunningLocalMode()) + "equipment_sensors")

    equipment_sensors.createOrReplaceTempView("equipment_sensors")
    equipments.createOrReplaceTempView("equipments")
    equipment_failure_sensors.createOrReplaceTempView("equipment_failure_sensors")

    (equipment_failure_sensors, equipments, equipment_sensors)
  }

  def computeTotalOfFailure(equipmentFailureSensors: DataFrame): Long = {equipmentFailureSensors.count()}

  def computeTotalEquipmentFailuresThatHappened() = {
    import spark.implicits._

    val df = spark.sqlContext.sql("select eq.equipment_id, count(eq.equipment_id) count_failures " +
                                              "from equipment_failure_sensors as efs " +
                                                "inner join equipment_sensors as es " +
                                                  "on efs.sensorID = es.sensor_id " +
                                                "inner join equipments as eq " +
                                                  "on es.equipment_id = eq.equipment_id " +
                                                "group by eq.equipment_id order by eq.equipment_id")

    val listTotalFailure = df.select($"count_failures").agg(sum($"count_failures")).collect()
    val mostEquipmentFailure = df.select($"equipment_id", $"count_failures").orderBy(desc("count_failures")).limit(1).collect()

    (mostEquipmentFailure :+ listTotalFailure(0))
  }

  def computeWhichEquipmentCodeHadMostFailures() = {
    import spark.implicits._

    val df = spark.sqlContext.sql("select eq.equipment_id, eq.code, " +
                                           "count(eq.equipment_id) as count_failures " +
                                            "from equipment_failure_sensors as efs " +
                                              "inner join equipment_sensors as es " +
                                                "on efs.sensorID = es.sensor_id " +
                                              "inner join equipments as eq " +
                                                "on es.equipment_id = eq.equipment_id " +
                                              "group by eq.equipment_id, eq.code " +
                                              "order by eq.equipment_id")

    df.select($"equipment_id", $"code")
      .orderBy(desc("count_failures"))
      .limit(1).collect()
  }

  def computeAverageAmountOfFailuresAcrossEquipmentGroup() = {
    import spark.implicits._

    val amountFailures = spark.sqlContext.sql("select eq.equipment_id, efs.sensorID, eq.code, eq.group_name " +
                                                        "from equipment_failure_sensors as efs " +
                                                          "inner join equipment_sensors as es " +
                                                            "on efs.sensorID = es.sensor_id " +
                                                          "inner join equipments as eq " +
                                                            "on es.equipment_id = eq.equipment_id")

    amountFailures.groupBy($"code", $"equipment_id", $"sensorID", $"group_name")
      .agg(avg($"code").as("avg_code"))
      .repartition(1)
      .select($"code", $"equipment_id", $"sensorID", $"group_name")
      .where($"avg_code" isNotNull)
      .orderBy(asc("sensorID")).collect()
  }
}
