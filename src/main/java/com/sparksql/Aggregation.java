package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

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

        Column score = col("score");
        Column subject = col("subject");
        // doesn't work due to poor API design, use agg instead
        // dataset.groupBy(subject).max(score.cast(DataTypes.IntegerType)).show(false);
        dataset.groupBy(subject).agg(
                max(score).cast(DataTypes.IntegerType).alias("highest"),
                min(score).alias("lowest"))
                .show(false);

        // pivot
        dataset.groupBy(subject).pivot("year")
                .agg(round(avg(score), 2).alias("avg"), round(stddev(score), 2).alias("std"))
                .show(false);
    }
}
