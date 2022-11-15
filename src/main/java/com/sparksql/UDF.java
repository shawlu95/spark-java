package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class UDF {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sc = SparkSession.builder()
                .appName("sparksql practice")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sc.read()
                .option("header", true)
                .option("inferSchema", true) // treat score as numeric
                .csv("src/main/resources/exams/students.csv");

        // add a static column
        dataset.withColumn("pass", lit("YES"));

        // add a dynamic column
        dataset.withColumn("ace", lit("grade").equalTo("A+")).show(10);

        // register a UDF using spark session object
        sc.udf().register("ace", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);
        dataset.withColumn("aced", callUDF("ace", col("grade"))).show(10);

        // more complex udf
        sc.udf().register("pass", (String grade) -> {
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);

        // multi-param udf
        sc.udf().register("complex_pass", (String grade, String subject) -> {
            if (subject.equals("Biology")) {
                return grade.startsWith("A");
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);
    }
}
