package com.shape.test.data

import com.shape.test.data.core.{parserEngine}
import com.typesafe.scalalogging.LazyLogging

object dataEngineerShapeTestApp extends App with LazyLogging {

  override def main(args: Array[String]): Unit = {
    print("Loading the Application...")

    //Load the entire data and create the data lake.
    parserEngine.initializeDataLake()



    println("The Test Finishes!!!")
  }
}
