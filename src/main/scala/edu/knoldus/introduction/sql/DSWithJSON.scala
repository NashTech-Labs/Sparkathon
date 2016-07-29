package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object DSWithJSON extends App{

  val spark = SparkSession.builder().master("local").appName("BigApple")
    .enableHiveSupport().getOrCreate()


  val peopleDS = spark.read.json("src/main/resources/people.json")

  peopleDS.printSchema()

  peopleDS.show()

  peopleDS.createOrReplaceTempView("PeopleTable")

  val queriedDS =
    spark.sql("SELECT name FROM PeopleTable WHERE age >= 13 AND age <= 19")

  queriedDS foreach (println(_))
}
