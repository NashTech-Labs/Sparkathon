package edu.knoldus.introduction.sql;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class ProgrammaticSchema {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("BigApple").getOrCreate();

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> people = spark.read().textFile("src/main/resources/people.txt").javaRDD();

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = people.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return RowFactory.create(fields[0], fields[1].trim());
                    }
                });

        // Apply the schema to the RDD.
        Dataset peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> results = spark.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.javaRDD().map(row1->"Name: " + row1.getString(0)).collect();

        System.out.println(names);

    }
}

