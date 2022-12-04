package com.ml;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
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
        csv = csv.withColumn("sqft_above_pcf",
                        col("sqft_above").divide(col("sqft_living")))
                .withColumnRenamed("price", "label");

        Dataset<Row>[] array = csv.randomSplit(new double[] { 0.8, 0.2 }, 0);
        Dataset<Row> trainVal = array[0];
        Dataset<Row> test = array[1];

        StringIndexer conditionIndexer = new StringIndexer()
                .setInputCol("condition")
                .setOutputCol("conditionIndex");

        StringIndexer gradeIndexer = new StringIndexer()
                .setInputCol("grade")
                .setOutputCol("gradeIndex");

        StringIndexer zipcodeIndexer = new StringIndexer()
                .setInputCol("zipcode")
                .setOutputCol("zipcodeIndex");

        OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
                .setInputCols(new String[] { "conditionIndex", "gradeIndex", "zipcodeIndex"})
                .setOutputCols(new String[] { "conditionVector", "gradeVector", "zipcodeVector"});

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "bedrooms", "bathrooms", "sqft_living",
                        "sqft_above_pcf", "floors", "grade",
                        "conditionVector", "gradeVector", "zipcodeVector" })
                .setOutputCol("features");

        // trainValSplit is a stage that memorizes model and params search grid
        LinearRegression regressor = new LinearRegression();
        ParamGridBuilder gridBuilder = new ParamGridBuilder();
        ParamMap[] paramMaps = gridBuilder
                .addGrid(regressor.regParam(), new double[] { 0.01, 0.1, 0.5 })
                .addGrid(regressor.elasticNetParam(), new double[] { 0, 0.5, 1.0 })
                .build();
        TrainValidationSplit trainValSplit = new TrainValidationSplit()
                .setEstimator(regressor)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMaps)
                .setTrainRatio(0.8);

        // assemble pipeline and fit on training set
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {
                        conditionIndexer,
                        gradeIndexer,
                        zipcodeIndexer,
                        encoder,
                        assembler,
                        trainValSplit
                });
        PipelineModel pipelineModel = pipeline.fit(trainVal);

        // extract model from pipeline
        TrainValidationSplitModel model = (TrainValidationSplitModel) pipelineModel.stages()[5];
        LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();

        // apply the fitted pipeline to test set, a "prediction" column is added
        test = pipelineModel.transform(test);
        test.show(10);
        test = test.drop("prediction"); // avoid exception with evaluate(test) later

        System.out.println("Training set R2:" + lrModel.summary().r2());
        System.out.println("Training set RMSE:" + lrModel.summary().rootMeanSquaredError());

        System.out.println("Test set R2:" + lrModel.evaluate(test).r2());
        System.out.println("Test set RMSE:" + lrModel.evaluate(test).rootMeanSquaredError());

        System.out.println("coeffs:" + lrModel.coefficients());
        System.out.println("intercept:" + lrModel.intercept());
        System.out.println("reg param:" + lrModel.getRegParam());
        System.out.println("elastic net param:" + lrModel.getElasticNetParam());
    }
}
