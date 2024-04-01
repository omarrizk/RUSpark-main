package com.RUSpark;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/* any necessary Java packages here */

public class NetflixMovieAverage {
	private static final Pattern COMMA = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: NetflixMovieAverage <file>");
			System.exit(1);
		}

		String InputPath = args[0];

		/* Implement Here */ 
		SparkSession spark = SparkSession
			.builder()
			.appName("NetflixMovieAverage")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();

		JavaPairRDD<Integer, Tuple2<Double, Integer>> impacts = lines.mapToPair(line -> {
			List<String> words = Arrays.asList(COMMA.split(line));
			return new Tuple2<>(
				Integer.parseInt(words.get(0)),
				new Tuple2<Double, Integer>(Double.parseDouble(words.get(2)),1)
			);
		});

		JavaPairRDD<Integer, Tuple2<Double, Integer>> counts = impacts.reduceByKey((i1, i2) -> new Tuple2<Double, Integer>(i1._1() + i2._1(), i1._2() + i2._2()));

		JavaPairRDD<Integer, Double> ratings = counts.mapToPair(rating -> {
			var average = rating._2()._1() / rating._2()._2();
			return new Tuple2<>(rating._1(), average);
		});

		ArrayList<Tuple2<Integer, Double>> output = new ArrayList<Tuple2<Integer, Double>>(ratings.collect());

		output.sort(Comparator.comparing(t -> t._2()));

		DecimalFormat decimalFormat = new DecimalFormat("#.##");
		for (Tuple2<Integer, Double> tuple : output) {
			System.out.println(
				new StringBuilder(tuple._1().toString())
					.append(" ")
					.append(decimalFormat.format(tuple._2()))
			);
		}

		spark.stop();
	}

}
