package edu.knoldus.introduction.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class WorkingWithHive {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("Sparkathon").enableHiveSupport().getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)");
        spark.sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src");

        // Queries are expressed in HiveQL.
        Dataset results = spark.sql("FROM src SELECT key, value");
        results.show();
    }
}
