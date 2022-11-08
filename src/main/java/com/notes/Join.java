package com.notes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Join {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4, 18));
        visitsRaw.add(new Tuple2<>(6, 4));
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> userRaw = new ArrayList<>();
        userRaw.add(new Tuple2<>(1, "John"));
        userRaw.add(new Tuple2<>(2, "Bob"));
        userRaw.add(new Tuple2<>(3, "Alan"));
        userRaw.add(new Tuple2<>(4, "Doris"));
        userRaw.add(new Tuple2<>(5, "Shaw"));
        userRaw.add(new Tuple2<>(6, "Ava"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(userRaw);

        JavaPairRDD<Integer, Tuple2<Integer, String>> inner = visits.join(users);
        inner.foreach(x -> System.out.println(x));

        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> left = visits.leftOuterJoin(users);
        left.foreach(x -> System.out.println(x._2._2.orElse("Blank").toUpperCase()));

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> right = visits.rightOuterJoin(users);
        right.foreach(x -> System.out.println(x._2._1.orElse(0)));

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cross = visits.cartesian(users);
        cross.foreach(it -> System.out.println(it));
    }
}
