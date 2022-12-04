package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitor {
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

        /* apply indexing to categorical feature
        +------------+------+---+------+------+--------+-----------+
        |CompetitorID|Gender|Age|Height|Weight|NoOfReps|GenderIndex|
        +------------+------+---+------+------+--------+-----------+
        |           1|     M| 23|   180|    88|      55|        0.0|
        |           5|     F| 24|   180|    78|      52|        1.0|
        |           6|     M| 25|   171|    89|      60|        0.0|
        |           7|     F| 18|   179|    80|      54|        1.0|
        +------------+------+---+------+------+--------+-----------+*/
        StringIndexer genderIndexer = new StringIndexer()
                .setInputCol("Gender")
                .setOutputCol("GenderIndex");
        csv = genderIndexer.fit(csv).transform(csv);

        /* transfer indexed feature to one-hot vector
        |CompetitorID|Gender|Age|Height|Weight|NoOfReps|GenderIndex| GenderVector|
        +------------+------+---+------+------+--------+-----------+-------------+
        |           1|     M| 23|   180|    88|      55|        0.0|(1,[0],[1.0])|
        |           5|     F| 24|   180|    78|      52|        1.0|    (1,[],[])|
        |           6|     M| 25|   171|    89|      60|        0.0|(1,[0],[1.0])|
        |           7|     F| 18|   179|    80|      54|        1.0|    (1,[],[])|
        +------------+------+---+------+------+--------+-----------+-------------+*/
        OneHotEncoderEstimator genderEncoder = new OneHotEncoderEstimator()
                .setInputCols(new String[] { "GenderIndex" })
                .setOutputCols(new String[] { "GenderVector" });
        csv = genderEncoder.fit(csv).transform(csv);

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
