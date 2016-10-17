package com.shell.hadoop.spark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

/**
 * 从输入文件计算URL的PageRank值.
 * 输入文件格式:
 * URL来源	URL目标
 * URL		neighbor URL
 * URL		neighbor URL
 * ...
 * URL来源和URL目标的意思是: 通过URL来源的url网页中的某个超类可以链到URL目标
 * @author Administrator
 *
 */
public class PageRank {
	private static final Pattern SPACES = Pattern.compile("\\s+");
	
	static void showWarning() {
		String warning = "WARN: This is a naive implementation of PageRank " +
	            "and is given as an example! \n" +
	            "Please use the PageRank implementation found in " +
	            "org.apache.spark.graphx.lib.PageRank for more conventional use.";
		System.err.println(warning);
	}
	
	private static class Sum implements Function2<Double, Double, Double> {

		@Override
		public Double call(Double v1, Double v2) throws Exception {
			return v1 + v2;
		}
	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
			System.exit(1);
		}
		
		showWarning();
		
		SparkSession spark = SparkSession.builder().appName("PageRank").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String t) throws Exception {
				String[] parts = SPACES.split(t);
				return new Tuple2<>(parts[0], parts[1]);
			}
		}).distinct().groupByKey().cache();
		
		JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {

			@Override
			public Double call(Iterable<String> v1) throws Exception {
				return 1.0;
			}
		});
		
		for (int current = 0; current < Integer.parseInt(args[1]); current++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
				@Override
				public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> t) throws Exception {
					int urlCount = Iterables.size(t._1());
					List<Tuple2<String, Double>> results = new ArrayList<>();
					for (String n : t._1) {
						results.add(new Tuple2<>(n, t._2() / urlCount));
					}
					return results.iterator();
				}
			});
			
			ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {

				@Override
				public Double call(Double v1) throws Exception {
					return 0.15 + v1 * 0.85;
				}
			});
		}
		
		List<Tuple2<String, Double>> output = ranks.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
		}
		
		spark.stop();
	}
}
