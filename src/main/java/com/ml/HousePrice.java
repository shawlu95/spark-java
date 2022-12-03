package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePrice {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("HousePrice")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/kc_house_data.csv");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{ "bedrooms", "bathrooms", "sqft_living" })
                .setOutputCol("features");

        Dataset<Row> dataset = assembler
                .transform(csv)
                .select("price", "features")
                .withColumnRenamed("price", "label");

        Dataset<Row>[] array = dataset.randomSplit(new double[] { 0.8, 0.5}, 0);
        Dataset<Row> train = array[0];
        Dataset<Row> test = array[1];

        LinearRegressionModel model = new LinearRegression().fit(train);
        model.transform(test).show(10);
    }
}
