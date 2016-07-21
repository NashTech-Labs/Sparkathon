package edu.knoldus.introduction.sql

import org.apache.spark.{SparkConf, SparkContext}

object ParquetWrite extends App {

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  case class Persona(name: String, age: Int)

  val persons = List(Persona("vikas", 10), Persona("Ravi", 12))

  val personRDD = sc.parallelize(persons).toDF()


  //personRDD.write.parquet("parquetPerson")

  val parquetFile = sqlContext.read.parquet("parquetPerson")

  val df = sqlContext.sql("SELECT * FROM parquet.parquetPerson")

  //println("****" + df.queryExecution.logical)

  df foreach (println(_))


}
