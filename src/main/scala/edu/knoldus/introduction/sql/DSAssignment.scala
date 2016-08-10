package edu.knoldus.introduction.sql

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object GlobalData {
  val spark = SparkSession.builder().appName("Assignment2").master("local[*]").config("spark.sql.shuffle.partitions", 8).getOrCreate()

  def dataFrame(path: String): DataFrame = spark.read.option("header", true).csv(path)
}

object FireDepartment {
  import GlobalData._

  private val callTypeColName = "Call Type"
  private val callDateTimestampColName = "Call Date Timestamp"

  val fireServiceCallsDF = dataFrame("src/main/resources/Fire_Department_Calls_for_Service.csv")
  val fireServiceCallsTSDF = fireServiceCallsDF.withColumn(callDateTimestampColName, unix_timestamp(fireServiceCallsDF("Call Date"), "MM/dd/yyyy")
    .cast("timestamp"))

  val callsTSDF = fireServiceCallsTSDF(callDateTimestampColName)


  def callTypes: Long = fireServiceCallsDF.select(callTypeColName).distinct().count()

  def callTypeIncidents: DataFrame = fireServiceCallsDF.select(callTypeColName).groupBy(callTypeColName).count()

  def yearsOfServiceCalls: Long = fireServiceCallsTSDF.select(year(callsTSDF)).distinct().count()

  // Spark SQL Encoder for java.sql.Timestamp
  implicit val timestampEncoder = Encoders.kryo[Timestamp]

  val currentTimeStamp = new Timestamp(Calendar.getInstance().getTime.getTime)
  val maxDateInData = fireServiceCallsTSDF.select(max(callsTSDF)).map(_.getTimestamp(0)).collect().headOption.fold(currentTimeStamp)(identity)
  val maxLocalDateTime = maxDateInData.toLocalDateTime


  def lastSevenDaysCalls: Long = fireServiceCallsTSDF.filter(year(callsTSDF) === maxLocalDateTime.getYear)
    .filter(dayofyear(callsTSDF) >= (maxLocalDateTime.getDayOfYear - 6)).groupBy(dayofyear(callsTSDF))
    .count().select(sum("count")).first().getLong(0)

  def topNeighbourhood: String = fireServiceCallsTSDF.filter(year(callsTSDF) === (maxLocalDateTime.getYear - 1)).groupBy("Neighborhood  District").count()
    .orderBy(desc("count")).select("Neighborhood  District").first().getString(0)
}

object DSAssignment extends App {
  import FireDepartment._

  println(s"No. of different types of calls made to Fire Department = $callTypes")
  println("No. of incidents of each call type are as follows:")
  callTypeIncidents.show(callTypes.toInt)

  println(s"No. of years of Fire Service Calls in data = $yearsOfServiceCalls")
  println(s"No. of service calls logged in last 7 days = $lastSevenDaysCalls")
  println(s"The neighbourhood in SF that generated most calls last year is: $topNeighbourhood")
}
