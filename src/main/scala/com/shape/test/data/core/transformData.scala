package com.shape.test.data.core

import com.shape.test.data.config.environment
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.util.Either
import scala.util.matching.{Regex, UnanchoredRegex}

object transformData extends SparkSessionWrapper {

  case class FailureLog(datetime: String, status: String, sensorID: Int, temperature: String, vibration: String)

  case class Equipment(equipment_id: Int, code: String, group_name: String)

  case class Sensor(equipment_id: String, sensor_id: String)

  private val DATETIME_PATTERN = """(?<=\[).*?(?=\])""".r
  private val SENSORID_PATTERN = """(?<=\[).*?(?=\])""".r
  private val FILTERED_DATE_PATTERN = "2020-01"

  val equipmentSchema = StructType(Seq(
    StructField("equipment_id", IntegerType, true),
    StructField("code", StringType, true),
    StructField("group_name", StringType, true)))

  def equipmentCharger(): Unit = {
    spark.read
      .schema(equipmentSchema)
      .json(environment.getSourceJsonSource(isLocal = environment.isRunningLocalMode()))
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(environment.DataLakeLocation(isLocal = environment.isRunningLocalMode()) + "/equipments")
  }

  def equimentSensorsCharger(): Unit = {
    spark.read
      .option("header", true)
      .options(Map("delimiter" -> ";"))
      .csv(environment.getSourceCsvSource(isLocal = environment.isRunningLocalMode()))
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(environment.DataLakeLocation(isLocal = environment.isRunningLocalMode()) + "/equipment_sensors")
  }

  def failuresEquipmentSensorsCharger(failureEquipmentsSensors: Seq[FailureLog]): Unit = {
    import spark.implicits._

    failureEquipmentsSensors.toDF().where($"datetime".contains(FILTERED_DATE_PATTERN))
      .write
      .mode(saveMode = SaveMode.Overwrite)
      .parquet(environment.DataLakeLocation(isLocal = environment.isRunningLocalMode()) + "/equipment_failure_sensors")
  }

  def failuresEquipmentSensorsLoader(): Array[String] = {
    spark.sparkContext
      .textFile(environment
        .getSourceLogsSource(isLocal = environment.isRunningLocalMode())).collect()
  }

  def loadContents(logs: Seq[String]) = {
    val parsedLines = logs.map(transformData.parseLine _)
    val requestLogs = parsedLines.filter(_.isRight).map(_.right.get)
    requestLogs
  }

  private def parseLine(line: String): Either[String, FailureLog] = {
    line match {
      case _ =>
        val dataLog = line.split("\\t")

        val datetime = DATETIME_PATTERN.findFirstIn(dataLog(0)).get
        val status = dataLog(1)
        val sensorid = SENSORID_PATTERN.findFirstIn(dataLog(2)).get.toInt
        val temperature = dataLog(4).replace(", vibration", "")
        val vibration = dataLog(5).replace(")", "")

        Right(FailureLog(datetime, status, sensorid, temperature, vibration))
    }
  }
}

