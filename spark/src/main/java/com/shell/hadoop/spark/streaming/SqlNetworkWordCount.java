package com.shell.hadoop.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SqlNetworkWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length < 2) {
			System.err.println("Usage: SqlNetworkWordCount <hostname> <port>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("SqlNetWordCount");
		@SuppressWarnings("resource")
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2);
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		});
		
		words.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {

			@Override
			public void call(JavaRDD<String> rdd, Time time) throws Exception {
				SparkSession spark = SparkSessionSingleton.getInstance(rdd.context().getConf());
				
				JavaRDD<Record> rowRDD = rdd.map(new Function<String, Record>() {

					@Override
					public Record call(String word) throws Exception {
						Record record = new Record();
						record.setWord(word);
						return record;
					}
					
				});
				
				Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, Record.class);
				
				wordsDataFrame.createOrReplaceTempView("words");
				
				Dataset<Row> wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word");
				System.out.println("=========" + time + "================");
				wordCountsDataFrame.show();
//				wordCountsDataFrame.write().format("json").save("file:/usr/local/sqlnetworkwordcount.json");
			}
			
		});
		
		ssc.start();
		ssc.awaitTermination();
	}
}

class SparkSessionSingleton {
	private static transient SparkSession instance = null;
	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		
		return instance;
	}
}
