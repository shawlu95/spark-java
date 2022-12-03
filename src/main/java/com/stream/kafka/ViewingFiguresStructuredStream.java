package com.stream.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class ViewingFiguresStructuredStream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("ViewingFiguresStructuredStream")
                .getOrCreate();

        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        // start dataframe operation
        df.createOrReplaceTempView("t");

        // key, value, timestamp
        Dataset<Row> results = session.sql("select cast(value as string) as course_name from t");

        StreamingQuery query = results.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start();

        query.awaitTermination();
    }
}
