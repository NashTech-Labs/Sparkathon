package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class BaseSQL {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        Dataset df = sqlContext.read().json("src/main/resources/people.json");
        df.show();

    }
}
