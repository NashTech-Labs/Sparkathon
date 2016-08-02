package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object DFWithJSON extends App {
  val spark = SparkSession.builder().master("local").appName("BigApple")
    .enableHiveSupport().getOrCreate()

  val peopleDF = spark.read.json("src/main/resources/people.json")

  peopleDF.printSchema()

  peopleDF.show()

  peopleDF.createOrReplaceTempView("PeopleTable")

  val queriedDF = spark.sql("SELECT name FROM PeopleTable WHERE age >= 13 AND age <= 19")

  queriedDF foreach (println(_))
}
