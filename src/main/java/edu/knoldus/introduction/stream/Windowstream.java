package edu.knoldus.introduction.stream;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.regex.Pattern;

public class Windowstream {
    public static void main(String[] args) throws Exception {

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

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        wordsDstream.print();

        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };

        JavaPairDStream<String, Integer> windowedWordCounts = wordsDstream.reduceByKeyAndWindow(reduceFunc, Durations.seconds(30), Durations.seconds(10));

        windowedWordCounts.print();


        ssc.start();
        ssc.awaitTermination();

    }
}
