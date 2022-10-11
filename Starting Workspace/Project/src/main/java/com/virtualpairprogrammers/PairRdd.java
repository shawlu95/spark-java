package com.virtualpairprogrammers;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PairRdd {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Haha");
        inputData.add("ERROR: Hello");
        inputData.add("FATAL: Hell");
        inputData.add("WARN: good");

        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        SparkConf conf = new SparkConf().setAppName("PairRdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> origMessages = sc.parallelize(inputData);
        JavaPairRDD<String, String> pairRdd = origMessages.mapToPair(msg -> {
            String[] cols = msg.split(":");
            String level = cols[0];
            String log = cols[1];
            return new Tuple2<>(level, log);
        });
        sc.close();
    }
}
