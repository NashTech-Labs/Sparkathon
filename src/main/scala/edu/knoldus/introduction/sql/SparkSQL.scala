package edu.knoldus.introduction.sql

import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL extends App {
  /*


    val df = sqlContext.read.json("src/main/resources/people.json")

    df.show()

    df.select(df("name"), df("age") + 1).show()

    df.select("name").show()
  */

  val conf = new SparkConf().setAppName("BigApple").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  val ds = Seq(1, 2, 3).toDS()
  ds.map(_ + 1).foreach(x => println(x))

}
