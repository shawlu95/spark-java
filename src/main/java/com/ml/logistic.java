package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class logistic {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("GymCompetitor")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .csv("src/main/resources/ml/GymCompetition.csv");

        csv.show(10);
    }
}
