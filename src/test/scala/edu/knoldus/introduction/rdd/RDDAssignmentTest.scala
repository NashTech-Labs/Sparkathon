package edu.knoldus.introduction.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAssignmentTest extends App{

  val conf = new SparkConf().setAppName("Big Apple").setMaster("local[4]")
  val sc = new SparkContext(conf)

  val pageCountsRdd = sc.textFile("src/test/resources/testPages.csv")

  // 10 records
  pageCountsRdd.take(100) foreach println

  //Total Records
  println(s"Total records = ${pageCountsRdd.count}")

  //Emglish Pages
  private val englishPages = pageCountsRdd.filter(line => line.contains("/en/"))

  println(s" English pages=${englishPages.count()} ")

  private val combineByKeyRDD: RDD[(Char, Int)] = pageCountsRdd.map(x => (x(1), x(2).toInt))

  private val pageCountGreaterThan200K: RDD[(Char, Int)] = combineByKeyRDD.reduceByKey(_ + _).filter{case (pageName,pageCount) => pageCount > 1}

  println(s" Pages Requested > 200k =${pageCountGreaterThan200K.count()} ")





  sc.stop()

}
