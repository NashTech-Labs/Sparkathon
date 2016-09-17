package edu.knoldus.introduction.structured

import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCount extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local")
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}
