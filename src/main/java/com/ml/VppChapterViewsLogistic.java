package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class VppChapterViewsLogistic {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("VppChapterViewsLogistic")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/vppChapterViews/*.csv");

        // make binary label, 1 if not watching next month
        csv = csv.filter("is_cancelled = false")
                .drop("observation_date", "is_cancelled")
                .withColumn("firstSub",
                        when(col("firstSub").isNull(), 0)
                                .otherwise(col("firstSub")))
                .withColumn("all_time_views",
                        when(col("all_time_views").isNull(), 0)
                                .otherwise(col("all_time_views")))
                .withColumn("last_month_views",
                        when(col("last_month_views").isNull(), 0)
                                .otherwise(col("last_month_views")))
                .withColumn("next_month_views",
                        when(col("next_month_views").$greater(0), 0)
                                .otherwise(1))
                .withColumnRenamed("next_month_views", "label");

        StringIndexer paymethodIndexer = new StringIndexer()
                .setInputCol("payment_method_type")
                .setOutputCol("payIndex");

        StringIndexer countryIndexer = new StringIndexer()
                .setInputCol("country")
                .setOutputCol("countryIndex");

        StringIndexer periodIndexer = new StringIndexer()
                .setInputCol("rebill_period_in_months")
                .setOutputCol("periodIndex");

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
                .setInputCols(new String[] { "payIndex", "countryIndex", "periodIndex" })
                .setOutputCols(new String[] { "payVector", "countryVector", "periodVector" });

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {
                        "firstSub", "age", "all_time_views", "last_month_views",
                        "payVector", "countryVector", "periodVector"})
                .setOutputCol("features");


    }
}
