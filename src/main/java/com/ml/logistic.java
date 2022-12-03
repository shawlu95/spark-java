package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
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
                .option("inferSchema", true)
                .csv("src/main/resources/ml/GymCompetition.csv");

        // all inputs to VectorAssembler must be numeric, not string
        VectorAssembler assembler = new VectorAssembler();
        assembler.setInputCols(new String[]{ "Age", "Height", "Weight" });
        assembler.setOutputCol("features");
        Dataset<Row> dataset = assembler
                .transform(csv)
                .select("NoOfReps", "features");
    }
}
