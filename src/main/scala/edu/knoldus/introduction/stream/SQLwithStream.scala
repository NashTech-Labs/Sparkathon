package edu.knoldus.introduction.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object SQLwithStream extends App {

  val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
  if (!log4jInitialized) {
    // We first log something to initialize Spark's default logging, then we override the
    // logging level.
    /*log4jInitialized("Setting log level to [WARN] for streaming example." +
      " To override add a custom log4j.properties to the classpath.")*/
    Logger.getRootLogger.setLevel(Level.WARN)
  }
  val conf = new SparkConf().setAppName("BigApple").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))

  val stream = ssc.textFileStream("src/main/resources/stream")
  val words = stream.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  wordCounts.print()

  words.foreachRDD { rdd =>

    // Get the singleton instance of SQLContext
    val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
    import sqlContext.implicits._

    // Convert RDD[String] to DataFrame
    val wordsDataFrame = rdd.toDF("word")

    // Register as table
    wordsDataFrame.registerTempTable("words")

    // Do word count on DataFrame using SQL and print it
    val wordCountsDataFrame =
      sqlContext.sql("select word, count(*) as total from words group by word")
    wordCountsDataFrame.show()
  }

  ssc.start()
  ssc.awaitTermination()


}
