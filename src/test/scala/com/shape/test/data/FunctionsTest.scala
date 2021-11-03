package com.shape.test.data

import com.shape.test.data.core.parserEngine._
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

import scala.collection.mutable.Stack

class ExampleTest extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session")
      .config("spark.some.config.option", true)
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "1g")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.cores.max", "1")
      .getOrCreate()
  }

  def fixture =
    new {
      //Load the entire data and create the data lake.
      initializeDataLake()

      loadDataFromParquet()
    }

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    }
  }

  it should "be Equipment Failure Sensors created with success" in {
    val f = fixture

    val equipment_failure_sensors = spark.read.parquet("./datalake/equipment_failure_sensors")
    equipment_failure_sensors.createOrReplaceTempView("equipment_failure_sensors")
    assert(equipment_failure_sensors.collect().length == 11645)
  }

  it should "be equipments created with success" in {
    val f = fixture

    val equipments = spark.read.parquet("./datalake/equipments")
    equipments.createOrReplaceTempView("equipments")
    assert(equipments.collect().length == 14)
  }

  it should "be equipment sensors created with success" in {
    val f = fixture

    val equipment_sensors = spark.read.parquet("./datalake/equipment_sensors")
    equipment_sensors.createOrReplaceTempView("equipment_sensors")
    assert(equipment_sensors.collect().length == 100)
  }

  it should "be question1 answered with success" in {
    val f = fixture

    val question1 = computeTotalEquipmentFailuresThatHappened()
    assert(question1.length == 2)
  }

  it should "be question2 answered with success" in {
    val f = fixture
    val question2 = computeWhichEquipmentCodeHadMostFailures()
    assert(question2.length == 1)
  }

  it should "be question3 answered with success" in {
    val f = fixture

    val question3 = computeAverageAmountOfFailuresAcrossEquipmentGroup()
    assert(question3.length == 8)
  }
}