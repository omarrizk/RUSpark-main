package com.RUSpark;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class RedditHourImpact {
	private static final Pattern COMMA = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			.builder()
			.appName("RedditHourImpact")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		SimpleDateFormat dateFormat = new SimpleDateFormat("H");
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-5"));

		JavaPairRDD<Integer, Integer> impacts = lines.mapToPair(line -> {
			List<String> words = Arrays.asList(COMMA.split(line));
			return new Tuple2<>(
				Integer.parseInt(dateFormat.format(new Date(Long.parseLong(words.get(1)) * 1000L))),
				Integer.parseInt(words.get(4)) + Integer.parseInt(words.get(5)) + Integer.parseInt(words.get(6))
			);
		});
		
		JavaPairRDD<Integer, Integer> counts = impacts.reduceByKey((i1, i2) -> i1 + i2);

		ArrayList<Tuple2<Integer, Integer>> output = new ArrayList<Tuple2<Integer, Integer>>(counts.collect());

		output.sort(Comparator.comparing(t -> t._1()));

		int i = 0;
		for (Tuple2<Integer, Integer> tuple : output) {
			System.out.println(tuple._1() == i ?
				new StringBuilder(tuple._1().toString())
					.append(" ")
					.append(tuple._2().toString()) :
				new StringBuilder(i)
					.append(" 0")
			);
			i++;
		}

		spark.stop();
	}

}
