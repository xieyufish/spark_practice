package com.shell.hadoop.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.out.println("Usage: WordCount <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession.builder().appName("WordCount").getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		});
		
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<>(t, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple : output) {
			System.out.println(tuple._1() + ":" + tuple._2());
		}
		
		spark.stop();
	}
	
}
