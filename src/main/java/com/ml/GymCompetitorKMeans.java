package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitorKMeans {
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

        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("Gender")
                .setOutputCol("GenderIndex");
        csv = genderIndexer.fit(csv).transform(csv);

        OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator()
                .setInputCols(new String[] { "GenderIndex" })
                .setOutputCols(new String[] { "GenderVector" });
        csv = genderEncoder.fit(csv).transform(csv);

        // all inputs to VectorAssembler must be numeric, not string
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{ "GenderVector", "Age", "Height", "Weight", "NoOfReps" })
                .setOutputCol("features");
        Dataset<Row> dataset = assembler
                .transform(csv)
                .select("features");

        // we don't separate train & test set with unsupervised learning
        KMeans kMeans = new KMeans().setK(3);
        KMeansModel model = kMeans.fit(dataset);
        Dataset<Row> pred = model.transform(dataset);
        pred.show(10);

        Vector[] clusterCenters = model.clusterCenters();
        for (Vector v : clusterCenters) { System.out.println(v); };
    }
}
