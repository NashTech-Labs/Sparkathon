package edu.knoldus.introduction.stream;

import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

public class SQLonStreams{
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        final Pattern SPACE = Pattern.compile(" ");

        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaDStream<String> lines = ssc.textFileStream("src/main/resources/stream");
        lines.print();

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x)).iterator();
            }
        });

        words.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> rdd, Time time) {

                        // Get the singleton instance of SQLContext
                        SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

                        // Convert RDD[String] to RDD[case class] to DataFrame
                        JavaRDD<JavaRecord> rowRDD = rdd.map(new Function<String, JavaRecord>() {
                            public JavaRecord call(String word) {
                                JavaRecord record = new JavaRecord();
                                record.setWord(word);
                                return record;
                            }
                        });
                        Dataset<Row> wordsDataFrame = sqlContext.createDataFrame(rowRDD, JavaRecord.class);

                        // Register as table
                        wordsDataFrame.registerTempTable("words");

                        // Do word count on table using SQL and print it
                        Dataset wordCountsDataFrame =
                                sqlContext.sql("select word, count(*) as total from words group by word");
                        wordCountsDataFrame.show();
                    }
                }
        );


        ssc.start();
        ssc.awaitTermination();

    }
}
