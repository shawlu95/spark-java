package com.virtualpairprogrammers;


import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
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
        JavaPairRDD<String, Long> pairRdd = origMessages.mapToPair(msg -> {
            String[] cols = msg.split(":");
            String level = cols[0];
            return new Tuple2<>(level, 1L);
        });

        // groupByKey can lead to performance issue on real-world dataset
        // due to hot keys with much more data than cold keys
        JavaPairRDD<String, Iterable<Long>> group = pairRdd.groupByKey();

        // guava is a transitive dependency of apache spark, can directly use
        group.foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));;

        JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((a, b) -> a + b);
        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // one liner
        sc.parallelize(inputData)
            .mapToPair(raw -> new Tuple2<>(raw.split(":")[0], 1))
            .reduceByKey(Integer::sum)
            .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // flat map: convert value to an iterator, cannot be an array such as String[]
        JavaRDD<String> words = origMessages.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
        words.foreach(x -> System.out.println(x));

        sc.close();
    }
}
