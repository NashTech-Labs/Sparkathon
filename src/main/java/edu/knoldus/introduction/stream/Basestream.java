package edu.knoldus.introduction.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Basestream {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<String> stringJavaDStream = ssc.textFileStream("src/main/resources/stream");
        stringJavaDStream.print();

        ssc.start();
        ssc.awaitTermination();

    }
}
