package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class Main {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger logger = Logger.getLogger(Main.class);

		SparkSession sc = SparkSession.builder()
				.appName("sparksql practice")
				.master("local[*]")
				.getOrCreate();

		Dataset<Row> dataset = sc.read()
				.option("header", true)
				.csv("src/main/resources/exams/students.csv");

		dataset.show();
		logger.info("Number of rows: " + dataset.count());

		Row first = dataset.first();
		logger.info(first.get(0));
		logger.info(first.get(1));
		logger.info(first.get(first.fieldIndex("subject")).toString());
		logger.info(first.get(first.fieldIndex("year")));
		logger.info(first.getAs("grade").toString());

		// all internal types of csv are string, auto conversion doesn't work
		// int year = first.getAs("year");
		int year_ = Integer.parseInt(first.getAs("year"));

		// -----------------------------------------------------------------
		// HOW TO FILTER
		Dataset<Row> math = dataset.filter("subject = 'Math' and score > 70");
		math.show(10);

		// use function, may be clumsy
		// Dataset<Row> top = dataset.filter((Function1<Row, Object>) x -> x.getAs("grade").equals("grade"));
		// top.show(10);

		// spark sql idiom
		Column subject = dataset.col("subject");
		Column year = dataset.col("year");
		Dataset<Row> modernArt = dataset.filter(subject.equalTo("Modern Art").and(year.equalTo(2007)));
		modernArt.show(10);

		// or import a static method
		Column subjectCol = col("subject");
		Column yearCol = col("year");
		dataset.filter(col("subject").equalTo("Math").and(col("year").gt(2010))).show(10);

		// -----------------------------------------------------------------
		// FULL SQL SYNTAX
		dataset.createOrReplaceTempView("students");
		Dataset<Row> agg = sc.sql("select subject, count(*) from students where grade = 'A+' group by subject");
		agg.show(100);

		sc.sql("select year, count(distinct student_id) as student_count from students group by year order by year").show(100);

		// a more realistic dataset
		Dataset<Row> ds = sc.read().option("header", true).csv("src/main/resources/extra/bigLog.txt");
		ds.createOrReplaceTempView("biglog");
		Dataset<Row> grouped = sc.sql("select level, " +
				"date_format(datetime, 'MM') mon, " +
				"date_format(datetime, 'MMMM') as month, " +
				"count(*) as total " +
				"from biglog group by 1, 2, 3 order by 1, 2").drop("mon");

		grouped.show(false);

		grouped.createOrReplaceTempView("grouped");
		sc.sql("select sum(total) from grouped").show();

		// -----------------------------------------------------------------
		// JAVA API

		ds.select("level", "datetime").show(10);

		// fragmented sql
		ds.selectExpr("level", "date_format(datetime, 'MMMM') as month").show(10);

		Dataset<Row> select = ds.select(
				col("level"),
				date_format(col("datetime"), "MMMM").alias("month"),
				date_format(col("datetime"), "M").alias("mon").cast(DataTypes.IntegerType)
		);
		select = select.groupBy(col("level"), col("month"), col("mon")).count();
		select.orderBy(col("level"), col("mon")).drop("mon").show(false);
	}
}
