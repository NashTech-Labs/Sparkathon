package edu.knoldus.introduction.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object StarterTry extends App {


  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)

  // Parallelized Collections

  val data = Array(1, 2, 3, 4, 5, 4)
  val distData = sc.parallelize(data)

  // Working with K-Vs
  val colOfTuples = distData.map((x) => (x, 1))

  val kk = colOfTuples.reduceByKey((a, b) => a + b)

  kk foreach (println)



  println(distData.reduce((a, b) => a + b))

  //closure case
  var counter = 10
  distData.foreach(a => counter = a + counter)
  println(s" The closure example result = $counter")


  //Creating RDD from a file

  val distFile = sc.textFile("src/main/resources/compressed.gz")
  val linesRDD = distFile.map(x => (x.length))
  val yaSumOfLines = linesRDD.reduce((a, b) => a + b)
  val sumOfLines = linesRDD.reduce(FunctionHolder.addFunc)
  println(sumOfLines, yaSumOfLines)

  object FunctionHolder {
    val addFunc = (a: Int, b: Int) => a + b
  }

}


class MyClass {
  def func1(s: String): String = "Hello"

  def doStuff(rdd: RDD[String]): RDD[String] = {
    rdd.map(func1)
  }
}

