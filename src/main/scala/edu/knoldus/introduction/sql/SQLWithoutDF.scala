package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object SQLWithoutDF extends App{

  val spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate()
  
  val ds = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")

  ds foreach (println(_))

}
