package com.shape.test.data.api

import com.shape.test.data.api.contracts.ContractOperations
import com.shape.test.data.core.parserEngine.{computeAverageAmountOfFailuresAcrossEquipmentGroup, computeTotalEquipmentFailuresThatHappened, computeWhichEquipmentCodeHadMostFailures, loadDataFromParquet}
import com.shape.test.data.exceptions.ExtractLogException
import org.apache.http.HttpStatus
import org.apache.spark.sql.Row

object shapeTestAPIs extends ContractOperations {

  def formatQuestions(question1: Array[Row], question2: Array[Row], question3: Array[Row]) = {
    val sb: StringBuilder = new StringBuilder()

    sb.append("Shape Test DataEngineer Results: \n\n")
    sb.append("The FPSO vessel contains some equipment and each equipment have multiple sensors. \nEvery time a failure happens, " +
      "we get all the sensors data from the failed equipment, \nand we store this information in a log file (the time is in GMT time zone). \n\n")
    sb.append("You are provided 3 files: the log file named “equipment_failure_sensors.log”; \nthe file named “equipment_sensors.csv” with " +
      "the relationships between sensors and equipment’s; \nand the file named “equipment.json” with the equipment data. \n\n")
    sb.append("To solve this problem, we expect you to answer a few questions related to January 2020 (considering GMT time zone): \n\n\n")
    sb.append("1) Total equipment failures that happened? \n")

    sb.append("/--------------------------------------------------/\n")
    sb.append("|Equipment ID          |             Count Failures|\n")
    sb.append(s"| ${question1(0).get(0)}                    |              ${question1(0).get(1)}         |\n")
    sb.append("|--------------------------------------------------|\n")
    sb.append(s"|Total of Log Analized: ${question1(1).get(0)}    Only January 2020 |\n")
    sb.append("/--------------------------------------------------/\n")
    sb.append("The Entire Log File has been 36979 Registries\n\n\n")

    sb.append("2) Which equipment code had most failures? \n\n")

    sb.append("/--------------------------------------------------/\n")
    sb.append("|Equipment ID          |             Code          |\n")
    sb.append(s"| ${question2(0).get(0)}                    |              ${question2(0).get(1)}     |\n")
    sb.append("/--------------------------------------------------/\n\n\n")

    sb.append("3) Average amount of failures across equipment group, ordered by the number of failures in ascending order? \n\n")

    sb.append("/----------------------------------------------------------------------------------------------/\n")
    sb.append("|Equipment ID          |             Serial ID |            Code      |       Group Name       |\n")
    sb.append("/----------------------------------------------------------------------------------------------/\n")
    question3.foreach(row => {
      sb.append(s"| ${row.get(1)}                    |              ${row.get(2)}        |             ${row.get(0)}|         ${row.get(3)}       |\n")
    })
    sb.append("/----------------------------------------------------------------------------------------------/\n\n\n")

    sb.toString()
  }

  override def computeDataFailuresFromLake(): (String, Int) = {
    try {
      loadDataFromParquet()

      val question1 = computeTotalEquipmentFailuresThatHappened()
      val question2 = computeWhichEquipmentCodeHadMostFailures()
      val question3 = computeAverageAmountOfFailuresAcrossEquipmentGroup()

      val httpResponse = formatQuestions(question1, question2, question3)

      (httpResponse, HttpStatus.SC_OK)
    } catch {
      case e: ExtractLogException => (e.getMessage(), HttpStatus.SC_BAD_REQUEST)
      case e: Exception => throw ExtractLogException()
    }
  }
}
