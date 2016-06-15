package edu.knoldus.introduction.rdd

import org.apache.spark.{SparkContext, SparkConf}

object YARdd extends App{

  val conf = new SparkConf().setAppName("BigApple").setMaster("local[2]")
  val sc = new SparkContext(conf)


  val anRDD = sc.parallelize(1 to 10)

  anRDD foreach println


}
