package com.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalytics {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("LogStreamAnalytics");

        // define batch size (duration) at context
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> inputData =
                jsc.socketTextStream("localhost", 8989);

        // a discretized, long series of RDD, but works just like a regular RDD
        // every 5 seconds, a batch is processed, and the stream is aggregated over last 24 batches
        // i.e. 24 * 5s-batches in a two-minute window
        JavaDStream<String> results = inputData.map(item -> item);
        JavaPairDStream<String, Long> pairDStream = results
                .map(msg -> msg.split(",")[0])
                .mapToPair(msg -> new Tuple2<String, Long>(msg, 1L))
                .reduceByKeyAndWindow(Long::sum, Durations.minutes(2)); // define window at aggregation
        pairDStream.print();

        jsc.start();
        jsc.awaitTermination(); // manual termination
    }
}
