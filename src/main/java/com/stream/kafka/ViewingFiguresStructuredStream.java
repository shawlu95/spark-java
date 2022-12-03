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

        // default is 200 partitions, but we only have 50 course key! reduce partition number
        session.conf().set("spark.sql.shuffle.partitions", 4);

        Dataset<Row> df = session.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .load();

        // start dataframe operation
        df.createOrReplaceTempView("t");

        // key, value, timestamp
        // full aggregation on "infinite" growing table, no window
        Dataset<Row> results = session.sql(
                "select window, cast(value as string) as course_name, " +
                        "sum(5) watch_time " +
                        "from t group by window(timestamp, '2 minutes'), course_name");

        StreamingQuery query = results.writeStream()
                .format("console")
                .outputMode(OutputMode.Complete())
                .option("truncate", false)
                .option("numRows", 50)
//                .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS)) // micro batch size, not recommended
                .start();

        query.awaitTermination();
    }
}
