package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object SQLWithoutDF extends App{

  val spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate()
  
  val df = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")

  df foreach (println(_))

}
