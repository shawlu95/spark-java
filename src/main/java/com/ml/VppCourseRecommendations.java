package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class VppCourseRecommendations {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("VppChapterViewsLogistic")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/VPPcourseViews.csv");
        csv = csv.withColumn("proportionWatched",
                col("proportionWatched").multiply(100));

        /**
         * The goal of recommender system is to predict the nulls
         * +------+------------------+------------------+-----------------+------------------+-----+------------------+------------------+------------------+-----------------+-----+-----+-----------------+-----+-----+-----------------+-----------------+-----+-----+-----+-----+
         * |userId|                 1|                 2|                3|                 4|    5|                 6|                 7|                 8|                9|   10|   11|               12|   13|   14|               15|               16|   17|   18|   19|   20|
         * +------+------------------+------------------+-----------------+------------------+-----+------------------+------------------+------------------+-----------------+-----+-----+-----------------+-----+-----+-----------------+-----------------+-----+-----+-----+-----+
         * |   496|              null|              62.0|             51.0|              35.0|  9.0|              26.0|              null|              39.0|             53.0| 94.0|100.0|             97.0| 83.0| 78.0|7.000000000000001|             23.0| null| null|100.0| 98.0|
         * |   471|28.000000000000004|              74.0|             63.0|              63.0| 48.0|              19.0|              33.0|              19.0|             null| null| null|             22.0| 17.0| null|             66.0|             97.0| 59.0| 18.0| null| 91.0|
         * |    53|             100.0|              88.0|             10.0|              null| null|              null|              null|              null|             null| null| null|             12.0| null| null|             50.0|             60.0| 34.0| 23.0| null| 25.0|
         * |   481|              null|              18.0|56.00000000000001|              61.0| 41.0|              null|              18.0|             100.0|             80.0|100.0| 94.0|            100.0| 31.0| 23.0|             72.0|             72.0| null| null| 51.0| 50.0|
         * +------+------------------+------------------+-----------------+------------------+-----+------------------+------------------+------------------+-----------------+-----+-----+-----------------+-----+-----+-----------------+-----------------+-----+-----+-----+-----+*/
        csv.groupBy("userId").pivot("courseId").sum("proportionWatched").show(5);

        Dataset<Row>[] arr = csv.randomSplit(new double[] { 0.9, 0.1 });
        Dataset<Row> train = arr[0];
        Dataset<Row> test = arr[1];

        // spark api needs three columns: user, item, rating (label)
        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol("userId")
                .setItemCol("courseId")
                .setRatingCol("proportionWatched");
        ALSModel model = als.fit(train);
        model.setColdStartStrategy("drop");

        Dataset<Row> pred = model.transform(test);
        pred.show(10);

        //TODO: resolve "java.io.NotSerializableException: scala.reflect.api.TypeTags$PredefTypeCreator"
        Dataset<Row> userRecs = model.recommendForAllUsers(5);
        List<Row> userRecsList = userRecs.takeAsList(10);
        for (Row rec : userRecsList) {
            int userId = rec.getAs(0);
            String recs = rec.getAs(1).toString();
            System.out.println("user:" + userId + " may like:" + recs);
            System.out.println("user already watched:");
            csv.filter("userId = " + userId).show();
        }

        //TODO: resolve "java.io.NotSerializableException: scala.reflect.api.TypeTags$PredefTypeCreator"
        Dataset<Row> testData = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/VppCourseViewsTest.csv");
        testData.show();

        model.transform(test).show();
        model.recommendForUserSubset(testData, 5).show();
    }
}
