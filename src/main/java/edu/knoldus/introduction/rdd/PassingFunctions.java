package edu.knoldus.introduction.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class PassingFunctions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        class GetLength implements Function<String, Integer> {
            public Integer call(String s) {
                return s.length();
            }
        }

        class Sum implements Function2<Integer, Integer, Integer> {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        }

        JavaRDD<String> lines = sc.textFile("src/main/resources/compressed.gz");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        // Printing an RDD
        lineLengths.foreach(x-> System.out.println(x));

        int totalLength = lineLengths.reduce(new Sum());

        System.out.println(totalLength);
    }
}
