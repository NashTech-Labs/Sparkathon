package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class WorkingWithHive {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // sc is an existing JavaSparkContext.
        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);

        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL.
        Row[] results = sqlContext.sql("FROM src SELECT key, value").collect();
        System.out.println(results);
    }
}
