package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object ParquetWrite extends App {
  val spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate()

  // this is used to implicitly convert an RDD to a Dataset.
  import spark.implicits._

  case class Person(name: String, age: Int)

  val persons = List(Person("vikas", 10), Person("Ravi", 12))

  val personDS = persons.toDS()

  //  personDS.write.parquet("parquetPerson")
  val parquetFile = spark.read.parquet("parquetPerson")

  val df = spark.sql("SELECT * FROM parquet.parquetPerson")

  //println("****" + df.queryExecution.logical)

  df foreach (println(_))
}
