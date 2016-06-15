package edu.knoldus.introduction.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class KVTuple {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/compressed.gz");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        counts.foreach(x-> System.out.println(x));
        JavaRDD<Integer> integerJavaRDD = counts.map(x -> x._2 + 10);
        integerJavaRDD.foreach(x-> System.out.println(x));

        //TODO what does counts.sortByKey do?
        // TODO what would counts.collect do?

    }
}
