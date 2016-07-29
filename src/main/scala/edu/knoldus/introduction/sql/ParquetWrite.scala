package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object ParquetWrite extends App {

  val spark =
    SparkSession.builder().master("local").appName("BigApple").getOrCreate()

  // this is used to implicitly convert an RDD to a DataFrame.
  import spark.implicits._

  case class Persona(name: String, age: Int)

  val persons = List(Persona("vikas", 10), Persona("Ravi", 12))

  val personRDD = spark.sparkContext.parallelize(persons).toDF()

  //personRDD.write.parquet("parquetPerson")
  val parquetFile = spark.read.parquet("parquetPerson")

  val ds = spark.sql("SELECT * FROM parquet.parquetPerson")

  //println("****" + df.queryExecution.logical)

  ds foreach (println(_))


}
