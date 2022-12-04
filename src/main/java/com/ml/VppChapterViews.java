package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class VppChapterViews {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("VppChapterViews")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csv = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/ml/vppChapterViews/*.csv");
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
                        when(col("next_month_views").isNull(), 0)
                                .otherwise(col("next_month_views")))
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

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {
                        paymethodIndexer,
                        countryIndexer,
                        periodIndexer,
                        encoder,
                        assembler
                });
        csv = pipeline
                .fit(csv)
                .transform(csv)
                .select("label", "features");
        Dataset<Row>[] arr = csv.randomSplit(new double[] { 0.9, 0.1 });
        Dataset<Row> trainVal = arr[0];
        Dataset<Row> test = arr[1];

        LinearRegression lr = new LinearRegression();
        ParamMap[] grid = new ParamGridBuilder()
                .addGrid(lr.regParam(), new double[] { 0.01, 0.1, 0.3, 0.5, 0.7, 1.0 })
                .addGrid(lr.elasticNetParam(), new double[] { 0.0, 0.5, 1.0 })
                .build();
        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(lr)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(grid)
                .setTrainRatio(0.9);

        TrainValidationSplitModel model = trainValidationSplit.fit(trainVal);
        LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();
        System.out.println("R2:" + lrModel.summary().r2());
        System.out.println("Test set R2:" + lrModel.evaluate(test).r2());
        System.out.println("intercept:" + lrModel.intercept());
        System.out.println("coeff:" + lrModel.coefficients());
        System.out.println("reg param:" + lrModel.getRegParam());
        System.out.println("elastic net param:" + lrModel.getElasticNetParam());
    }
}
