package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class DirectWithParquet {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        DataFrame df = sqlContext.read().json("src/main/resources/people.json");
        df.select("name", "age").write().mode(SaveMode.Overwrite).format("parquet").save("namesAndAges.parquet");
        DataFrame df2 = sqlContext.sql("SELECT * FROM parquet.`namesAndAges.parquet`");
        df2.show();
    }
}
