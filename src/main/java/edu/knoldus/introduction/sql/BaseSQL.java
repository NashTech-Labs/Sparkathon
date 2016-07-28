package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class BaseSQL {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("Sparkathon").getOrCreate();

        Dataset ds = spark.read().json("src/main/resources/people.json");
        ds.show();

    }
}
