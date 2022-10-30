package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// warm-up
		JavaPairRDD<Integer, Integer> chapterCount =
				chapterData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1)).reduceByKey((a, b) -> a + b);
		chapterCount.foreach(x -> System.out.println(x));

		// step 1: deduplicate chapter view
		viewData = viewData.distinct();
		viewData.foreach(x -> System.out.println(x));

		// step 2: get the course IDs into RDD
		viewData = viewData.mapToPair(row -> new Tuple2<Integer, Integer>(row._2, row._1));
		viewData.foreach(x-> System.out.println(x));

		// chapter ID -> (user, course)
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joined = viewData.join(chapterData);
		joined.foreach(x -> System.out.println(x));

		// step 3: get rid of chapter ID, group by (userId, courseId), count how many times a course is viewed
		JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joined.mapToPair(row -> {
			Integer userId = row._2._1;
			Integer courseId = row._2._2;
			return new Tuple2<>(new Tuple2<>(userId, courseId), 1L);
		});

		// step 4: count how many views for each course
		JavaPairRDD<Tuple2<Integer, Integer>, Long> step4 = step3.reduceByKey((a, b) -> a + b);
		step4.foreach(x -> System.out.println(x));

		// step 5: remove userId (courseId, view count)
		JavaPairRDD<Integer, Long> step5 = step4.mapToPair(x -> new Tuple2<>(x._1._2, x._2));
		step5.foreach(x -> System.out.println(x));

		// step 6: attached chapter count for each course
		JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step5.join(chapterCount);
		step6.foreach(x -> System.out.println(x));

		// step 7: convert to ratio, keep key, transform value
		JavaPairRDD<Integer, Double> step7 = step6.mapValues(x -> (double) x._1 / x._2);
		step7.foreach(x -> System.out.println(x));

		// step 8: convert to score, keep key, transform value
		JavaPairRDD<Integer, Integer> step8 = step7.mapValues(value -> {
			if (value > 0.9) return 10;
			if (value > 0.5) return 4;
			if (value > 0.25) return 2;
			return 0;
		});
		step8.foreach(x -> System.out.println(x));

		// step 9:
		JavaPairRDD<Integer, Integer> step9 = step8.reduceByKey((a, b) -> a + b);
		JavaPairRDD<Integer, Tuple2<Integer, String>> step10 = step9.join(titlesData);

		step10.mapToPair(row -> new Tuple2<Integer, String>(row._2._1, row._2._2))
				.sortByKey(false)
				.collect()
				.forEach(x -> System.out.println(x));

		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		if (testMode) {
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}

		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] cols = commaSeparatedLine.split(",");
					return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				});
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		if (testMode) {
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] cols = commaSeparatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
				});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		if (testMode) {
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				.mapToPair(commaSeparatedLine -> {
					String[] columns = commaSeparatedLine.split(",");
					return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				});
	}
}
