package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HousePriceExplore {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("HousePriceExplore")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/kc_house_data.csv");

        csv.describe().show(false);

        // ignore features that have weak correlation with label
        csv = csv.drop(
                "id", "date", "waterfront", "view",
                "condition", "grade", "yr_renovated",
                "zipcode", "lat", "long");
        for (String col : csv.columns()) {
            System.out.println("corr price-" + col + ":" + csv.stat().corr("price", col));
        }

        // remove correlated features: sqft_living, sqft_above, sqft_basement
        csv = csv.drop("sqft_lot", "sqft_lot15", "yr_built", "sqft_living15");
        for (String a : csv.columns()) {
            for (String b : csv.columns()) {
                System.out.println("corr" + a + "-" + b + ":" + csv.stat().corr(a, b));
            }
        }
    }
}
