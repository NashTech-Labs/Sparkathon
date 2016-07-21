package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object DFwithHive extends App{

  val hqlContext = SparkSession.builder().master("local").appName("Sparkathon").enableHiveSupport().getOrCreate()

  hqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
  hqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")

  // Queries are expressed in HiveQL
  hqlContext.sql("FROM src SELECT key, value").collect().foreach(println)



}
