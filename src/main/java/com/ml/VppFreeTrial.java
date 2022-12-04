package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class VppFreeTrial {
    public static UDF1<String,String> countryGrouping = new UDF1<String,String>() {

        @Override
        public String call(String country) throws Exception {
            List<String> topCountries =  Arrays.asList(new String[] {"GB","US","IN","UNKNOWN"});
            List<String> europeanCountries =  Arrays.asList(new String[] {"BE","BG","CZ","DK","DE","EE","IE","EL","ES","FR","HR","IT","CY","LV","LT","LU","HU","MT","NL","AT","PL","PT","RO","SI","SK","FI","SE","CH","IS","NO","LI","EU"});

            if (topCountries.contains(country)) return country;
            if (europeanCountries .contains(country)) return "EUROPE";
            else return "OTHER";
        }

    };

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("VppFreeTrial")
                .master("local[*]")
                .getOrCreate();
        spark.udf().register("countryGrouping", countryGrouping, DataTypes.StringType);

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/vppFreeTrials.csv");

        csv = csv.withColumn("country", callUDF("countryGrouping", col("country")))
                .withColumn("label", when(col("payments_made").geq(1), lit(1)).otherwise(lit(0)));

        // decision tree doesn't need to one-hot encode numeric categorical features
        csv = new StringIndexer()
                .setInputCol("country")
                .setOutputCol("countryIndex")
                .fit(csv)
                .transform(csv);

        // get a index-country mapping
        new IndexToString()
                .setInputCol("countryIndex")
                .setOutputCol("value")
                .transform(csv.select("countryIndex").distinct())
                .show();

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] { "countryIndex", "rebill_period", "chapter_access_count", "seconds_watched" })
                .setOutputCol("features");

        csv = assembler.transform(csv).select("label", "features");

        Dataset<Row>[] arr = csv.randomSplit(new double[] { 0.8, 0.2 });
        Dataset<Row> train = arr[0];
        Dataset<Row> test = arr[1];

        DecisionTreeRegressor dt = new DecisionTreeRegressor()
                .setMaxDepth(3);
        DecisionTreeRegressionModel model = dt.fit(train);

        model.transform(test).show(100);
        System.out.println(model.toDebugString());
    }
}
