package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.DoubleAccumulator;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<Double>();
        inputData.add(1.0);
        inputData.add(2.0);
        inputData.add(3.0);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("example")
                .setMaster("local[*]"); // fully utilize local machine
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Double> myRdd = sc.parallelize(inputData); // create RDD

        DoubleAccumulator acc = sc.sc().doubleAccumulator("foo");

        myRdd.foreach(acc::add);

        Double sum = myRdd.reduce((Double a, Double b) -> a + b);

        JavaRDD<Double> rootRdd = myRdd.map(Math::sqrt);
        Double rootSum = rootRdd.reduce(Double::sum);

        assert acc.value() == 6;

        // side effect, collect to avoid serialization error
         myRdd.collect().forEach(System.out::println);
         rootRdd.collect().forEach(System.out::println);

        System.out.println(sum);
        System.out.println(rootSum);
        System.out.println(acc.value());
        System.out.printf("How many item: %d%n", rootRdd.count());

        // dumb count
        Integer count = rootRdd.map(x -> 1).reduce(Integer::sum);
        System.out.printf("How many item: %d%n", count);

        sc.close();
    }
}
