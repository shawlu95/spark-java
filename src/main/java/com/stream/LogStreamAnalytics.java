package com.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class LogStreamAnalytics {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("LogStreamAnalytics");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> inputData =
                jsc.socketTextStream("localhost", 8989);

        // a discretized, long series of RDD, but works just like a regular RDD
        JavaDStream<String> results = inputData.map(item -> item);
        results.print();

        jsc.start();
        jsc.awaitTermination(); // manual termination
    }
}
