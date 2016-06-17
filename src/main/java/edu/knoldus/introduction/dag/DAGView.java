package edu.knoldus.introduction.dag;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DAGView {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/compressed.gz");
        JavaRDD<String[]> filteredRDD = lines.map(s -> s.split(" ")).filter(words -> words.length > 0);

        System.out.println(filteredRDD.toDebugString());

        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD =
                filteredRDD.mapToPair(words -> new Tuple2<>(words[0], 1));
        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD1 =
                stringIntegerJavaPairRDD.reduceByKey((a, b) -> a + b);

        System.out.println(stringIntegerJavaPairRDD1.toDebugString());

    }


}
