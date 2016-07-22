package edu.knoldus.introduction.sql

import org.apache.spark.{SparkConf, SparkContext}

object DFWithJSON extends App{

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val peopleDF = sqlContext.read.json("src/main/resources/people.json")

  peopleDF.printSchema()

  peopleDF.show()

  peopleDF.createOrReplaceTempView("PeopleTable")

  val queriedDF = sqlContext.sql("SELECT name FROM PeopleTable WHERE age >= 13 AND age <= 19")

  queriedDF foreach (println(_))
}
