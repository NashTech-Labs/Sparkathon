package edu.knoldus.introduction.sql

import org.apache.spark.{SparkConf, SparkContext}

object SQLWithoutDF extends App{

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val df = sqlContext.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")

  df foreach println

}
