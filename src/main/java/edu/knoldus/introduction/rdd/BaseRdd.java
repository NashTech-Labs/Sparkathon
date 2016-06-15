package edu.knoldus.introduction.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class BaseRdd {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data,10);

        distData.foreach(x->System.out.println(x));

        Integer reduce = distData.reduce((a, b) -> a + b);
        System.out.println(reduce);

        // Creating RDD from a file
        // =========================
        // TODO Try with different kind of files in the resources folder
        JavaRDD<String> lines = sc.textFile("src/main/resources/compressed.gz");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

        // TODO Where would the persist go?
        //lineLengths.persist(StorageLevel.MEMORY_ONLY());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);

    }
}
