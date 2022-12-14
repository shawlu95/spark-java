package com.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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

		// default is 200, which creates too much empty partition
		sc.conf().set("spark.sql.shuffle.partitions", "12");

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
		select.groupBy(col("level"), col("month"), col("mon"))
				.count()
				.orderBy(col("level"), col("mon"))
				.drop("mon")
				.show(false);

		// using level as row, month as column
		select.groupBy("level").pivot("month").count().show(false);

		// passing in a list of columns to specify order
		// if column name is not found will show null, can fill_na using .na().fill(val)
		Object[] cols = new Object[] {
				"January", "February", "March", "April", "May", "June", "July", "August",
				"September", "October", "November", "Decemberr"
		};
		List<Object> columns = Arrays.asList(cols);

		// fill na with a desired value
		select.groupBy("level").pivot("month", columns).count().na().fill(0).show(false);

		// UDF example
		SimpleDateFormat input = new SimpleDateFormat("MMMM");
		SimpleDateFormat output = new SimpleDateFormat("M");
		sc.udf().register("monthNum", (String month) -> {
			java.util.Date inputDate = input.parse(month);
			return Integer.parseInt(output.format(inputDate));
		}, DataTypes.IntegerType);

		// simplify SQL using UDF
		sc.sql("select level, " +
				"date_format(datetime, 'MMMM') as month, " +
				"count(*) as total " +
				"from biglog group by level, 2 order by monthNum(month), level");

		// hack: user the scanner to avoid terminating program
		// view Spark UI at: http://localhost:4040/jobs/
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
	}
}
