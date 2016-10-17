package com.shell.hadoop.spark.streaming;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.clearspring.analytics.util.Lists;

import scala.Tuple2;

public class QueueStream {
	private QueueStream() {
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("QueueStream");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		
		Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
		
		List<Integer> list = Lists.newArrayList();
		for (int i = 0; i < 1000; i++) {
			list.add(i);
		}
		
		for (int i = 0; i < 30; i++) {
			rddQueue.add(ssc.sparkContext().parallelize(list));
		}
		
		JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
		JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(new PairFunction<Integer, Integer, Integer>() {

			@Override
			public Tuple2<Integer, Integer> call(Integer t) throws Exception {
				return new Tuple2<>(t % 10, 1);
			}
			
		});
		
		JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		reducedStream.print();
		ssc.start();
		ssc.awaitTermination();
	}
}
