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

        assert acc.value() == 6;

        System.out.println(sum);
        System.out.println(acc.value());

        sc.close();
    }
}
