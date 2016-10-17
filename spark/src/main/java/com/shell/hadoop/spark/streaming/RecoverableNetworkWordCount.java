package com.shell.hadoop.spark.streaming;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.spark_project.guava.io.Files;

import scala.Tuple2;

class WordBlacklist {
	private static volatile Broadcast<List<String>> instance = null;
	
	public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (WordBlacklist.class) {
				if (instance == null) {
					List<String> wordBlacklist = Arrays.asList("a", "b", "c");
					instance = jsc.broadcast(wordBlacklist);
				} 
			}
		}
		
		return instance;
	}
}

class DroppedWordsCounter {
	private static volatile LongAccumulator instance = null;
	
	public static LongAccumulator getInstance(JavaSparkContext jsc) {
		if (instance == null) {
			synchronized (DroppedWordsCounter.class) {
				if (instance == null) {
					instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
				}
			}
		}
		
		return instance;
	}
}

public class RecoverableNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	private static JavaStreamingContext createContext(String ip, int port, String checkpointDirectory, String outputPath) {
		
		System.out.println("Creating new context");
		final File outputFile = new File(outputPath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("RecoverableNetworkWordCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		ssc.checkpoint(checkpointDirectory);
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(ip, port);
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
		
		wordCounts.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {

			@Override
			public void call(JavaPairRDD<String, Integer> rdd, Time time) throws Exception {
				final Broadcast<List<String>> blackList = WordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
				final LongAccumulator droppedWordsCounter = DroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
				
				String counts = rdd.filter(new Function<Tuple2<String, Integer>, Boolean>() {

					@Override
					public Boolean call(Tuple2<String, Integer> wordCount) throws Exception {
						if (blackList.value().contains(wordCount._1())) {
							droppedWordsCounter.add(wordCount._2());
							return false;
						} else {
							return true;
						}
					}
					
				}).collect().toString();
				
				String output = "Counts at time " + time + " " + counts;
				System.out.println(output);
				System.out.println("Dropped " + droppedWordsCounter.value() + " word(s) totally");
				System.out.println("Appending to " + outputFile.getAbsolutePath());
				Files.append(output + "\n", outputFile, Charset.defaultCharset());
			}
			
		});
		
		return ssc;
	}
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length != 4) {
			System.err.println("You arguments were " + Arrays.asList(args));
			System.err.println(
			          "Usage: JavaRecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>\n" +
			          "     <output-file>. <hostname> and <port> describe the TCP server that Spark\n" +
			          "     Streaming would connect to receive data. <checkpoint-directory> directory to\n" +
			          "     HDFS-compatible file system which checkpoint data <output-file> file to which\n" +
			          "     the word counts will be appended\n" +
			          "\n" +
			          "In local mode, <master> should be 'local[n]' with n > 1\n" +
			          "Both <checkpoint-directory> and <output-file> must be absolute paths");
			System.exit(1);
		}
		
		final String ip = args[0];
		final int port = Integer.parseInt(args[1]);
		final String checkpointDirectory = args[2];
		final String outputPath = args[3];
		
		Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {

			@Override
			public JavaStreamingContext call() throws Exception {
				return createContext(ip, port, checkpointDirectory, outputPath);
			}
			
		};
		
		JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
		ssc.start();
		ssc.awaitTermination();
		
	}
}
