package com.shell.hadoop.spark.sql;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;

/**
 * 用Dataset API接口计算wordCount问题
 * @author Administrator
 *
 */
public class WordCount {
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setAppName("SqlWordCount");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		
		Dataset<String> files = sparkSession.read().textFile("file:/usr/local/spark-2.0/table.txt");
		files.show();
		Dataset<String> words = files.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(SPACE.split(t)).iterator();
			}
			
		}, Encoders.STRING());
		words.show();
		
		KeyValueGroupedDataset<String, String> keyValueDataset = words.groupByKey(new MapFunction<String, String>() {

			@Override
			public String call(String value) throws Exception {
				return value;
			}
			
		}, Encoders.STRING());
		keyValueDataset.keys().show();
		keyValueDataset.count().show();
		
	}
}
