package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class DirectWithParquet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        Dataset ds = sqlContext.read().json("src/main/resources/people.json");
        ds.select("name", "age").write().mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet");
        Dataset ds2 = sqlContext.sql("SELECT * FROM parquet.`namesAndAges.parquet`");
        ds2.show();
    }
}
