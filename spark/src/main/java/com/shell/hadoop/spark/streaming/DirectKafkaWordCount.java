package com.shell.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


public class DirectKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
	          "  <brokers> is a list of one or more Kafka brokers\n" +
	          "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}
		
		String brokers = args[0];
		String topics = args[1];
		
		SparkConf sparkConf = new SparkConf().setAppName("DirectKafkaWordCount");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jsc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception {
				return tuple2._2();
			}
			
		});
		
		JavaDStream<String> words =lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>(){

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
