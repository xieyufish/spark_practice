package com.shell.hadoop.spark.sql.streaming;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import scala.Tuple2;

public class StructuredNetworkWordCountWindowed {
	public static void main(String[] args) {
		
		if (args.length < 3) {
			System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port> <window duration in seconds> [<slide duration in seconds>]");
			System.exit(1);
		}
		
		String host = args[0];
		int port = Integer.parseInt(args[1]);
		int windowSize = Integer.parseInt(args[2]);
		int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
		if (slideSize > windowSize) {
			System.err.println("<slide duration> must be less than or equal to <window duration>");
		}
		
		String windowDuration = windowSize + " seconds";
		String slideDuration = slideSize + " seconds";
		
		SparkSession spark = SparkSession.builder().appName("StructuredNetworkWordCountWindowed").getOrCreate();
		
		Dataset<Tuple2<String, Timestamp>> lines = spark.readStream().format("socket").option("host", host).option("port", port).option("includeTimestamp", true).load().as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()));
		
		Dataset<Row> words = lines.flatMap(new FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>() {

			@Override
			public Iterator<Tuple2<String, Timestamp>> call(Tuple2<String, Timestamp> t) throws Exception {
				List<Tuple2<String, Timestamp>> result = new ArrayList<>();
				for (String word : t._1().split(" ")) {
					result.add(new Tuple2<>(word, t._2));
				}
				return result.iterator();
			}
			
		}, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");
		
		Dataset<Row> windowedCounts = words.groupBy(functions.window(words.col("timestamp"), windowDuration, slideDuration), words.col("window")).count().orderBy("window");
		
		StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console").option("truncate", "false").start();
		query.awaitTermination();
	}
}
