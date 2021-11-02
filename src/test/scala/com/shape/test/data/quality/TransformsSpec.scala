package com.shape.test.data.quality

import org.scalatest.FunSpec
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.shape.test.data.config.environment
//
//class TransformsSpec
//    extends FunSpec
//    with SparkSessionTestWrapper
//    with DataFrameComparer {
//
//  import spark.implicits._
//
//  describe(".happyData") {
//
//    it("appends a happy column to a DataFrame") {
//
//      val sourceDF = Seq(
//        ("jose"),
//        ("li"),
//        ("luisa")
//      ).toDF("name")
//
//      val actualDF = sourceDF.transform(transforms.happyData())
//
//      val expectedData = List(
//        Row("jose", "data is fun"),
//        Row("li", "data is fun"),
//        Row("luisa", "data is fun")
//      )
//
//      val expectedSchema = List(
  //        StructField("name", StringType, true),
//        StructField("happy", StringType, false)
//      )
//
//      val expectedDF = spark.createDataFrame(
//        spark.sparkContext.parallelize(expectedData),
//        StructType(expectedSchema)
//      )
//
//      assertSmallDataFrameEquality(actualDF, expectedDF)
//
//    }
//
//    it("appends a new Structure to a DataFrame") {
//
//      case class Item(
//                       id: Long,
//                       productName: String,
//                       description: String,
//                       priority: String,
//                       numViews: Long
//                     )
//
//
//      val rdd = spark.sparkContext.parallelize(Seq(
//        Item(1, "Thingy A", "awesome thing.", "high", 0),
//        Item(2, "Thingy B", "available at http://thingb.com", null, 0),
//        Item(3, null, null, "low", 5),
//        Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
//        Item(5, "Thingy E", null, "high", 12)))
//
//      val data = spark.read.parquet(environment.HDFSDestinationFolder(isLocal = true))
//
//      val verificationResult = VerificationSuite()
//        .onData(data)
//        .addCheck(
//          Check(CheckLevel.Error, "unit testing my data")
//            .hasSize(_ == 5) // we expect 5 rows
//            .isComplete("id") // should never be NULL
//            .isUnique("id") // should not contain duplicates
//            .isComplete("productName") // should never be NULL
//            // should only contain the values "high" and "low"
//            .isContainedIn("priority", Array("high", "low"))
//            .isNonNegative("numViews") // should not contain negative values
//            // at least half of the descriptions should contain a url
//            .containsURL("description", _ >= 0.5)
//            // half of the items should have less than 10 views
//            .hasApproxQuantile("numViews", 0.5, _ <= 10))
//        .run()
//
//
//      if (verificationResult.status == CheckStatus.Success) {
//        println("The data passed the test, everything is fine!")
//      } else {
//        println("We found errors in the data:\n")
//
//        val resultsForAllConstraints = verificationResult.checkResults
//          .flatMap { case (_, checkResult) => checkResult.constraintResults }
//
//        resultsForAllConstraints
//          .filter { _.status != ConstraintStatus.Success }
//          .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
//      }
//    }
//  }
//}
