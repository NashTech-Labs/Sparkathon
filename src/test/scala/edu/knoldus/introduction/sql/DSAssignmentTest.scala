package edu.knoldus.introduction.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class DSAssignmentTest extends FunSuite with BeforeAndAfter {
  private val master = "local[*]"
  private val appName = "DS Assignment"
  private val callDateTimestampColName = "Call Date Timestamp"
  private var spark: SparkSession = _
  private var fireServiceCallsDF: Dataset[Row] = _
  private var fireServiceCallsTSDF: Dataset[Row] = _
  private var callsTSDF: Column = _

  import FireDepartment._

  before {
    spark = SparkSession.builder().appName(appName).master(master).config("spark.sql.shuffle.partitions", 8).getOrCreate()
    fireServiceCallsDF = spark.read.option("header", true).csv("src/test/resources/testFireDepartmentCalls.csv")
    fireServiceCallsTSDF = fireServiceCallsDF.withColumn(callDateTimestampColName, unix_timestamp(fireServiceCallsDF("Call Date"), "MM/dd/yyyy")
      .cast("timestamp"))
    callsTSDF = fireServiceCallsTSDF(callDateTimestampColName)
  }

  test("DSAssignment Test Scenarios") {
    assert(callTypes(fireServiceCallsDF) == 4)
    assert((callTypeIncidents(fireServiceCallsDF).count) == 4)
    assert(yearsOfServiceCalls(fireServiceCallsTSDF, callsTSDF) == 3)
    assert(lastSevenDaysCalls(fireServiceCallsTSDF, callsTSDF) == 2)
    assert(topNeighbourhood(fireServiceCallsTSDF, callsTSDF).isInstanceOf[String])
  }

  after {
    if (spark != null) {
      spark.stop()
    }
  }
}
