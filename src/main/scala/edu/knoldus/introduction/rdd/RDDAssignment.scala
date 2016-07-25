package edu.knoldus.introduction.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object RDDAssignment extends App{

  private val conf = new SparkConf().setAppName("Big Apple").setMaster("local[4]")
  private val sc = new SparkContext(conf)

  // Page Counts RDD
  private val pageCountsRdd = sc.textFile("src/main/resources/assignment/pagecounts-20151201-220000.gz")

  // 10 records
  pageCountsRdd.take(100) foreach println

  //Total Records
  println(s"Total records = ${pageCountsRdd.count}")

  //English Pages
  private val englishPages = pageCountsRdd.filter(line => line.contains("/en/"))

  // English Pages Count
  println(s" English pages = ${englishPages.count()}")

  // Requested > 200K
  private val combineByKeyRDD: RDD[(Char, Int)] = pageCountsRdd.map(x => (x(1), x(2).toInt))
  private val pageCountGreaterThan200K: RDD[(Char, Int)] = combineByKeyRDD.reduceByKey(_ + _).filter{case (pageName,pageCount) => pageCount > 200000}

  println(s" Pages Requested>200k =${pageCountGreaterThan200K.count()} ")

  sc.stop()
}
