package edu.knoldus.introduction.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.List;

public class ProgrammaticDataLoad {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Big Apple").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<Person> people = sc.textFile("src/main/resources/people.txt").map(
                new Function<String, Person>() {
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");

                        Person person = new Person();
                        person.setName(parts[0]);
                        person.setAge(Integer.parseInt(parts[1].trim()));

                        return person;
                    }
                });

        // Apply a schema to an RDD of JavaBeans and register it as a table.
        DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);

        // DataFrames can be saved as Parquet files, maintaining the schema information.
        schemaPeople.write().mode(SaveMode.Overwrite).parquet("people.parquet");

        // Read in the Parquet file created above. Parquet files are self-describing so the schema is preserved.
        // The result of loading a parquet file is also a DataFrame.
        DataFrame parquetFile = sqlContext.read().parquet("people.parquet");

        // Parquet files can also be registered as tables and then used in SQL statements.
        parquetFile.registerTempTable("parquetFile");
        DataFrame teenagers = sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
        List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        System.out.println(teenagerNames);
    }
}
