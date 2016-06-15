package edu.knoldus.introduction.stream

object JoinStream extends App {

  import org.apache.spark._
  import org.apache.spark.streaming._


  val conf = new SparkConf().setAppName("BigApple").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(2))

  val stream1 = ssc.textFileStream("src/main/resources/stream")
  val stream2 = ssc.textFileStream("src/main/resources/stream2")

  val words1 = stream1.flatMap(_.split(" "))
  val wordCounts1 = words1.map(x => (x, 1)).reduceByKey(_ + _)
  // Reduce last 30 seconds of data, every 10 seconds
  val windowedWordCounts1 = wordCounts1.reduceByKeyAndWindow((a: Int, b: Int) =>
    (a + b), Seconds(30), Seconds(10))

  windowedWordCounts1.print()

  val words2 = stream2.flatMap(_.split(" "))
  val wordCounts2 = words2.map(x => (x, 1)).reduceByKey(_ + _)
  // Reduce last 30 seconds of data, every 10 seconds
  val windowedWordCounts2 = wordCounts2.reduceByKeyAndWindow((a: Int, b: Int) =>
    (a + b), Seconds(30), Seconds(10))
  windowedWordCounts2.print()

  val joinedStream = windowedWordCounts1.join(windowedWordCounts2)

  joinedStream.print()

  ssc.start()
  ssc.awaitTermination()

}
