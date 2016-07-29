package edu.knoldus.introduction.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, SparkSession}

object DSWithRDDReflection extends App {

  val spark =
    SparkSession.builder().appName("BigApple").master("local").getOrCreate()

  // this is used to implicitly convert an RDD to a Dataset.
  case class Address(city: String, state: String, country: String)
  case class Person(name: String, age: Int, add: Address)

  //create an RDD from a file
  val peopleRDD = spark.sparkContext.textFile("src/main/resources/people.txt")

  val peopleSplitRDD = peopleRDD.map(_.split(","))
  val peopleRDDExtractPerson = peopleSplitRDD.map(p =>
    Person(p(0), p(1).trim.toInt, Address(p(2), p(3), p(4))))

  // this is used to implicitly convert an RDD to a Dataset.
  import spark.implicits._

  val peopleDS = peopleRDDExtractPerson.toDS()

  peopleDS foreach (println(_))
  peopleDS.createOrReplaceTempView("people")

  // SQL statements can be run by using the sql methods provided by sqlContext.
  val teenagers = spark.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
  val inPune = spark.sql("SELECT * FROM people where add.city='Pune'")

  inPune foreach (println(_))

  // The results of SQL queries are Datasets and support all the normal RDD operations.
  // The columns of a row in the result can be accessed by field index:
  teenagers.map(t => "Name: " + t(0)) foreach (println(_))

  // or by field name:
  teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
  // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
  // Primitive types and case classes can be also defined as
  implicit val stringIntMapEncoder: Encoder[Map[String, Int]] = ExpressionEncoder()

  teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  // Map("name" -> "Justin", "age" -> 19)


}
