package edu.knoldus.introduction.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;

public class JavaStructuredNetworkWordCount {
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .master("local")
                .config("spark.sql.shuffle.partitions", 8)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

       // Split the lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public Iterator<String> call(String x) {
                                return Arrays.asList(x.split(" ")).iterator();
                            }
                        }, Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
