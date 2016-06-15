package edu.knoldus.introduction.stream

object BaseStream extends App {

  import org.apache.spark._
  import org.apache.spark.streaming._


  val conf = new SparkConf().setAppName("BigApple").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(2))


  val lines = ssc.textFileStream("src/main/resources/stream")

  lines.foreachRDD(x => x foreach println)


  val words = lines.flatMap(_.split(" "))
  words.print()

  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  wordCounts.print()

  // Reduce last 30 seconds of data, every 10 seconds
  val windowedWordCounts = wordCounts.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10))

  windowedWordCounts.print()

  ssc.start()
  ssc.awaitTermination()
}

