package edu.knoldus.introduction.sql

import org.apache.spark.sql.SparkSession

object SparkSQL extends App {
  val spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate()

  /*
    val df = spark.read.json("src/main/resources/people.json")

    df.show()

    df.select(df("name"), df("age") + 1).show()

    df.select("name").show()
  */

  // this is used to implicitly convert an RDD to a Dataset.
  import spark.implicits._

  val ds = Seq(1, 2, 3).toDS()
  ds.map(_ + 1).foreach(x => println(x))
}
