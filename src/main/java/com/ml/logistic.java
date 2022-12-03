package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
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
                .select("NoOfReps", "features")
                .withColumnRenamed("NoOfReps", "label");

        LinearRegression lr = new LinearRegression();
        LinearRegressionModel model = lr.fit(dataset);
        double intercept = model.intercept();
        Vector coeffs = model.coefficients();
        System.out.println("intercept: " + intercept);
        System.out.println("coeffs: " + coeffs);

        model.transform(dataset).show(10);
    }
}
