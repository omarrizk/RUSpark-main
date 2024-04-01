package com.RUSpark;

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

public class NetflixGraphGenerate {
	private static final Pattern COMMA = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession
			.builder()
			.appName("NetflixGraphGenerate")
			.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
		
		// A tuple of ((movie, rating), customer)
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> impacts = lines.mapToPair(line -> {
			List<String> words = Arrays.asList(COMMA.split(line));
			return new Tuple2<>(
					new Tuple2<Integer, Integer>(
						Integer.parseInt(words.get(0)),
						Integer.parseInt(words.get(2))
					), 
					Integer.parseInt(words.get(1))
				);
		});

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> ones = impacts
			// Get unique tuples by movie and rating
			.cartesian(impacts)
			.filter((Tuple2<Tuple2<Tuple2<Integer,Integer>,Integer>,Tuple2<Tuple2<Integer,Integer>,Integer>> pair) ->
				pair._1()._1()._1() == pair._2()._1()._1() &&
				pair._1()._1()._2() == pair._2()._1()._2() &&
				pair._1()._2() < pair._2()._2()
			)
			// Map to customers by commonality
			.mapToPair(pair -> new Tuple2<>(
				new Tuple2<>(
					pair._1()._2(),
					pair._2()._2()
				),
				1
			));

		JavaPairRDD<Tuple2<Integer, Integer>, Integer> counts = ones.reduceByKey((i1,i2) -> i1 + i2);
		
		ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>(counts.collect());
		
		output.sort(
			Comparator.comparing((Tuple2<Tuple2<Integer, Integer>, Integer> t) -> t._2())
			.reversed()
			.thenComparing(t -> t._1()._1())
			.thenComparing(t -> t._1()._2())
		);
		
		for (Tuple2<Tuple2<Integer, Integer>, Integer> t : output) {
			System.out.println(
				new StringBuilder("(")
					.append(t._1()._1().toString())
					.append(", ")
					.append(t._1()._2().toString())
					.append(") ")
					.append(t._2().toString())
			);
		}
		
	}

}