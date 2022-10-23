package com.virtualpairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadDisk {
    public static boolean isInteresting(String word) {
        return !Objects.equals(word, "Java");
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARNING);
        SparkConf conf = new SparkConf().setAppName("PairRdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile("src/main/resources/subtitles/input-spring.txt");
        // escape backslash and add s to mean a "space"
        JavaRDD<String> lettersOnlyRdd = input.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

        // use "trim" to remove leading and trailing space
        JavaRDD<String> removedEmptyLine = lettersOnlyRdd.filter(s -> s.trim().length() > 0);
        JavaRDD<String> words = removedEmptyLine.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> cleaned = words.filter(w -> w.trim().length() > 0);
        JavaRDD<String> interesting = cleaned.filter(w -> !Util.isBoring(w));

        JavaPairRDD<String, Long> pairRDD = interesting.mapToPair(w -> new Tuple2<String, Long>(w, 1L));
        JavaPairRDD<String, Long> totals = pairRDD.reduceByKey(Long::sum);

        // switch key and value and sort by value
        JavaPairRDD<Long, String> switched = totals.mapToPair(pair -> new Tuple2<Long, String>(pair._2, pair._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false); // false for sorting desc

        // print top 10 keywords
        sorted.take(10).forEach(x -> System.out.println(x));
        sc.close();
    }
}
