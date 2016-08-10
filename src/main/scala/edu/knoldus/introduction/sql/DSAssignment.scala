package edu.knoldus.introduction.sql

import java.sql.Timestamp

import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object GlobalData {
  val spark = SparkSession.builder().appName("Assignment2").master("local[*]").config("spark.sql.shuffle.partitions", 8).getOrCreate()

  def getDataFrame(path: String): DataFrame = spark.read.option("header", true).csv(path)
}

object FireDepartment {
  private val callTypeColName = "Call Type"

  private def getMaxLocalDateTime(fireServiceCallsTSDF: DataFrame, callsTSDF: Column) =
    fireServiceCallsTSDF.select(max(callsTSDF)).first().getTimestamp(0).toLocalDateTime

  // Spark SQL Encoder for java.sql.Timestamp
  implicit val timestampEncoder = Encoders.kryo[Timestamp]

  def callTypes(fireServiceCallsDF: DataFrame): Long = fireServiceCallsDF.select(callTypeColName).distinct().count()

  def callTypeIncidents(fireServiceCallsDF: DataFrame): DataFrame = fireServiceCallsDF.select(callTypeColName).groupBy(callTypeColName).count()

  def yearsOfServiceCalls(fireServiceCallsTSDF: DataFrame, callsTSDF: Column): Long = fireServiceCallsTSDF.select(year(callsTSDF)).distinct().count()

  def lastSevenDaysCalls(fireServiceCallsTSDF: DataFrame, callsTSDF: Column): Long = {
    val maxLocalDateTime = getMaxLocalDateTime(fireServiceCallsTSDF, callsTSDF)

    fireServiceCallsTSDF.filter(year(callsTSDF) === maxLocalDateTime.getYear)
      .filter(dayofyear(callsTSDF) >= (maxLocalDateTime.getDayOfYear - 6)).groupBy(dayofyear(callsTSDF))
      .count().select(sum("count")).first().getLong(0)
  }

  def topNeighbourhood(fireServiceCallsTSDF: DataFrame, callsTSDF: Column): String = {
    val maxLocalDateTime = getMaxLocalDateTime(fireServiceCallsTSDF, callsTSDF)

    fireServiceCallsTSDF.filter(year(callsTSDF) === (maxLocalDateTime.getYear - 1)).groupBy("Neighborhood  District").count()
      .orderBy(desc("count")).select("Neighborhood  District").first().getString(0)
  }
}

object DSAssignment extends App {
  import FireDepartment._
  import GlobalData._

  private val callDateTimestampColName = "Call Date Timestamp"

  val fireServiceCallsDF = getDataFrame("src/main/resources/assignment/Fire_Department_Calls_for_Service.csv")
  val fireServiceCallsTSDF = fireServiceCallsDF.withColumn(callDateTimestampColName, unix_timestamp(fireServiceCallsDF("Call Date"), "MM/dd/yyyy")
    .cast("timestamp"))
  val callsTSDF = fireServiceCallsTSDF(callDateTimestampColName)

  private val callTypesValue = callTypes(fireServiceCallsDF)

  println(s"No. of different types of calls made to Fire Department = $callTypesValue")
  println("No. of incidents of each call type are as follows:")
  callTypeIncidents(fireServiceCallsDF).show(callTypesValue.toInt)

  println(s"No. of years of Fire Service Calls in data = ${yearsOfServiceCalls(fireServiceCallsTSDF, callsTSDF)}")
  println(s"No. of service calls logged in last 7 days = ${lastSevenDaysCalls(fireServiceCallsTSDF, callsTSDF)}")
  println(s"The neighbourhood in SF that generated most calls last year is: ${topNeighbourhood(fireServiceCallsTSDF, callsTSDF)}")
}
