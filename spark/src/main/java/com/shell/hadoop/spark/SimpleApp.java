package com.shell.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
	public static void main(String[] args) {
		String logFile = "/usr/local/spark-2.0/README.md";
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();
		
		long numAs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			public Boolean call(String s) { 
				return s.contains("a");
			}
		}).count();
		
		long numBs = logData.filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 4059697741945224605L;

			public Boolean call(String s) {
				return s.contains("b");
			}
		}).count();
		
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//		logData.map(s -> s.length()).reduce((a,b) -> a + b);
		sc.stop();
		sc.close();
	}
}
