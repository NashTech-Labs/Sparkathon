package edu.knoldus.introduction.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DirectWithParquet {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate();

        Dataset ds = spark.read().json("src/main/resources/people.json");
        ds.select("name", "age").write().mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet");

        Dataset ds2 = spark.sql("SELECT * FROM parquet.`namesAndAges.parquet`");
        ds2.show();
    }
}
