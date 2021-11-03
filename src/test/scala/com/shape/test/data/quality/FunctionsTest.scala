package com.shape.test.data.quality

import com.amazon.deequ.VerificationResult.checkResultsAsDataFrame
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstrainableDataTypes
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.Row

/**
 * FunctionsSpec Tests Applying Great Spectations
 */
//class FunctionsSpec
//    extends FunSpec
//    with SparkSessionTestWrapper
//    with ColumnComparer {
//
//  import spark.implicits._
//
//  describe("isEven") {
//
//    val data = transform.importDataInLake()
//
//    it("returns true if the number is even and false otherwise") {
//
//      val data = Seq(
//        (1, false),
//        (2, true),
//        (3, false)
//      )
//
//      val df = data
//        .toDF("some_num", "expected")
//        .withColumn("actual", functions.isEven(col("some_num")))
//
//      assertColumnEquality(df, "actual", "expected")
//
//    }
//
//    it("Calculate Overall Metrics of the Ingested Data") {
//      import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
//      import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
//      import com.amazon.deequ.analyzers.{Compliance, Correlation, Size, Completeness, Mean, ApproxCountDistinct}
//
//      val analysisResult: AnalyzerContext = { AnalysisRunner
//        // data to run the analysis on
//        .onData(data)
//        // define analyzers that compute metrics
//        .addAnalyzer(Size())
//        .addAnalyzer(Completeness("review_id"))
//        .addAnalyzer(ApproxCountDistinct("review_id"))
//        .addAnalyzer(Mean("Age_in_Yrs"))
//        .addAnalyzer(Compliance("Emp Age 18+", "Age_in_Yrs >= 18.0"))
//        .addAnalyzer(Correlation("Age_in_Yrs", "Weight_in_Kgs"))
//        .addAnalyzer(Correlation("Age_in_Company_Years", "Salary"))
//        // compute metrics
//        .run()
//      }
//      // retrieve successfully computed metrics as a Spark data frame
//      val metrics = successMetricsAsDataFrame(spark, analysisResult)
//      metrics.show()
//
//    }
//
//    it("Verify if Data Validation Passing with total success According Constraints") {
//
//      import spark.implicits._
//
//      val _check = Check(CheckLevel.Warning, "Data Validation Check")
//        .isComplete("First_Name")
//        .isComplete("Month_Name_of_Joining")
//        .isContainedIn("Month_Name_of_Joining", Array("August", "July", "January", "April", "December", "November", "February", "March", "June", "September", "May", "October"))
//        .isComplete("Phone_No")
//        .containsEmail("E_Mail")
//        .containsSocialSecurityNumber("SSN")
//        .isContainedIn("Day_of_Joining", 1, 31, includeLowerBound = true, includeUpperBound = true)
//        .hasPattern("Phone_No", """^[+]*[(]{0,1}[0-9]{1,4}[)]{0,1}[-\s\./0-9]*$""".r)
//        .hasDataType("Emp_ID", ConstrainableDataTypes.Integral)
//        .isUnique("Emp_ID")
//      val verificationResult: VerificationResult = { VerificationSuite()
//        .onData(data)
//        .addCheck(_check)
//        .run()
//      }
//
//
//      val resultDataFrame = checkResultsAsDataFrame(spark, verificationResult)
//      resultDataFrame.show()
//
//      val occurrencesNumber = resultDataFrame.filter($"constraint_status" === "Failure").count()
//
//      occurrencesNumber match {
//        case 0 => assert(true)
//        case _ => assert(false)
//      }
//    }
//
//  }
//
//}
