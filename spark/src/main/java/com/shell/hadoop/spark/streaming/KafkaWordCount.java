package com.shell.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class KafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	private KafkaWordCount() {
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 4) {
			System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jsc, args[0], args[1], topicMap);
		
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return tuple2._2();
			}
			
		});
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<>(t, 1);
			}
			
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		wordCounts.print();
		jsc.start();
		jsc.awaitTermination();
		
	}
}
