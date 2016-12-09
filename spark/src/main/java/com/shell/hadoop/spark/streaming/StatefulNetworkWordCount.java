package com.shell.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StatefulNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 2) {
			System.err.println("Usage: StatefulNetworkWordCount <hostname> <port>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount");
		@SuppressWarnings("resource")
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		ssc.checkpoint(".");
		
		List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
		JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		});
		
		JavaPairDStream<String, Integer> wordsDStream = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<>(t, 1);
			}
			
		});
		
		Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {

			@Override
			public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) throws Exception {
				int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
				Tuple2<String, Integer> output = new Tuple2<>(word, sum);
				state.update(sum);
				return output;
			}
			
		};
		
		JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDStream = wordsDStream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));
		
		stateDStream.print();
		ssc.start();
		ssc.awaitTermination();
		
	}
}
