package edu.knoldus.introduction.sql

import org.apache.spark.{SparkContext, SparkConf}

object DFwithHive extends App{

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)

  val hqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  hqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  hqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

  // Queries are expressed in HiveQL
  hqlContext.sql("FROM src SELECT key, value").collect().foreach(println)



}
