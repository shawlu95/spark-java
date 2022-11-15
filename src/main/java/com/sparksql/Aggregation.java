package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

public class Aggregation {
    @SuppressWarnings("resource")
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger logger = Logger.getLogger(Main.class);

        SparkSession sc = SparkSession.builder()
                .appName("sparksql practice")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dataset = sc.read()
                .option("header", true)
                .option("inferSchema", true) // treat score as numeric
                .csv("src/main/resources/exams/students.csv");

        dataset.groupBy("subject").max("score").show(false);

        Column score = functions.col("score");
        Column subject = functions.col("subject");
        // doesn't work due to poor API design, use agg instead
        // dataset.groupBy(subject).max(score.cast(DataTypes.IntegerType)).show(false);
        dataset.groupBy(subject).agg(
                functions.max(score).cast(DataTypes.IntegerType).alias("highest"),
                functions.min(score).alias("lowest"))
                .show(false);
    }
}
