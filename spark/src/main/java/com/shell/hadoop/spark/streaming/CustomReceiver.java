package com.shell.hadoop.spark.streaming;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.spark_project.guava.io.Closeables;

import scala.Tuple2;

public class CustomReceiver extends Receiver<String> {
	private static final Pattern SPACE = Pattern.compile(" ");
	private String host = null;
	private int port = -1;
	
	public CustomReceiver(String host_, int port_) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		host = host_;
		port = port_;
	}

	@Override
	public void onStart() {
		new Thread() {
			public void run() {
				receive();
			}
		}.start();
	}

	@Override
	public void onStop() {
		
	}
	
	private void receive() {
		try {
			Socket socket = null;
			BufferedReader reader = null;
			String userInput = null;
			
			try {
				socket = new Socket(host, port);
				reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
				while (!isStopped() && (userInput = reader.readLine()) != null) {
					System.out.println("Received data '" + userInput + "'");
					store(userInput);
				}
			} finally {
				Closeables.close(reader, true);
				Closeables.close(socket, true);
			}
			
//			restart("Trying to connect again");
		} catch (ConnectException e) {
			restart("Could not connect", e);
		} catch (Throwable e) {
			restart("Error receiving data", e);
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		if (args.length < 2) {
			System.err.println("Usage: CustomReceiver <hostname> <port>");
			System.exit(1);
		}
		
//		StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("CustomReceiver");
		@SuppressWarnings("resource")
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));
		
		JavaReceiverInputDStream<String> lines = ssc.receiverStream(new CustomReceiver(args[0], Integer.parseInt(args[1])));
		
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
		ssc.start();
		ssc.awaitTermination();
	}
}
