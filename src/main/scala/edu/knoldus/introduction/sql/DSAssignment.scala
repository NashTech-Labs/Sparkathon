package edu.knoldus.introduction.sql

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object DSAssignment extends App {
  val spark = SparkSession.builder().appName("Assignment2").master("local[*]").config("spark.sql.shuffle.partitions", 8).getOrCreate()

  import spark.implicits._

  val fireServiceCallsDF = spark.read.option("header", true).option("inferSchema", true)
    .csv("src/main/resources/Fire_Department_Calls_for_Service.csv")

  val callTypes = fireServiceCallsDF.select("Call Type").distinct().count()
  println(s"No. of different types of calls made to Fire Department = $callTypes")

  val callTypeIncidents = fireServiceCallsDF.select("Call Type").groupBy("Call Type").count()
  println("No. of incidents of each call type are as follows:")
  callTypeIncidents.show(callTypes.toInt)

  val fireServiceCallsTSDF = fireServiceCallsDF.withColumn("Call Date Timestamp", unix_timestamp(fireServiceCallsDF("Call Date"), "MM/dd/yyyy")
    .cast("timestamp"))

  val callsTSDF = fireServiceCallsTSDF("Call Date Timestamp")

  val yearsOfServiceCalls = fireServiceCallsTSDF.select(year(callsTSDF)).distinct().count()
  println(s"No. of years of Fire Service Calls in data = $yearsOfServiceCalls")

  implicit val timestampEncoder = Encoders.kryo[Timestamp]

  val currentTimeStamp = new Timestamp(Calendar.getInstance().getTime.getTime)
  val maxDateInData = fireServiceCallsTSDF.select(max(callsTSDF)).map(_.getTimestamp(0)).collect().headOption.fold(currentTimeStamp)(identity)
  val maxLocalDateTime = maxDateInData.toLocalDateTime

  val lastSevenDaysCalls = fireServiceCallsTSDF.filter(year(callsTSDF) === maxLocalDateTime.getYear)
    .filter(dayofyear(callsTSDF) >= (maxLocalDateTime.getDayOfYear - 6)).groupBy(dayofyear(callsTSDF))
    .count().select(sum("count")).map(_.getLong(0)).collect().headOption.fold(0L)(identity)

  println(s"No. of service calls logged in last 7 days = $lastSevenDaysCalls")

  val topNeighbourhood = fireServiceCallsTSDF.filter(year(callsTSDF) === (maxLocalDateTime.getYear - 1)).groupBy("Neighborhood  District").count()
    .orderBy(desc("count")).select("Neighborhood  District").collect().map(_.getString(0)).headOption.fold("")(identity)
  println(s"The neighbourhood in SF that generated most calls last year is: $topNeighbourhood")
}
