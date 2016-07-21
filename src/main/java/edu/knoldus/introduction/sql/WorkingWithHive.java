package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WorkingWithHive {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // sc is an existing JavaSparkContext.
        SparkSession sqlContext = SparkSession.builder().master("local").appName("Sparkathon").enableHiveSupport().getOrCreate();

        sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        sqlContext.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL.
        Dataset<Row> results = sqlContext.sql("FROM src SELECT key, value");
        results.show();
    }
}
