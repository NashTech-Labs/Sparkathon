package edu.knoldus.introduction.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.Predef.*;
import scala.math.Ordering;
import java.util.Arrays;
import java.util.List;

public class WorkingWithJSON {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate();

        // A JSON dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files.
        Dataset people = spark.read().json("src/main/resources/people.json");

        // The inferred schema can be visualized using the printSchema() method.
        people.printSchema();
        // root
        //  |-- age: integer (nullable = true)
        //  |-- name: string (nullable = true)

        // Register this Dataset as a table.
        people.registerTempTable("people");

        // SQL statements can be run by using the sql methods provided by sqlContext.
        Dataset teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // Alternatively, a Dataset can be created for a JSON dataset represented by
        // an RDD[String] storing one JSON object per string.
        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        Dataset anotherPeople = spark.createDataset(jsonData, Encoders.STRING());
        anotherPeople.show();
    }
}
