package com.shell.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class FlumeEventCount {
	private FlumeEventCount() {
		
	}
	
	public static void main(String[] args) throws InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: FlumeEventCount <host> <port>");
			System.exit(1);
		}
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		
		SparkConf sparkConf = new SparkConf().setAppName("FlumeEventCount");
		JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
		
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(jsc, host, port);
		flumeStream.count();
		
		flumeStream.count().map(new Function<Long, String>() {

			@Override
			public String call(Long in) throws Exception {
				return "Received " + in + " flume events";
			}
			
		}).print();
		
		jsc.start();
		jsc.awaitTermination();
	}
}
