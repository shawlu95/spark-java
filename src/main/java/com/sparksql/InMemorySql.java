package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * It's useful to create a dummy dataset for unit testing */
public class InMemorySql {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession sc = SparkSession.builder()
                .appName("sparksql practice")
                .master("local[*]")
                .getOrCreate();

        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));


        StructField[] fields = new StructField[] {
            new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty()),
        };
        StructType schema = new StructType(fields);
        Dataset<Row> df = sc.createDataFrame(inMemory, schema);

        df.show(10);

        df.createOrReplaceTempView("logging_table");
        sc.sql("select level, max(datetime) as latest from logging_table group by level").show(10);

        // hack: user the scanner to avoid terminating program
        // view Spark UI at: http://localhost:4040/jobs/
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }
}
