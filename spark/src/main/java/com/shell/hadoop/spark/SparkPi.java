package com.shell.hadoop.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


public class SparkPi {
	public static void main(String[] args) {
		JavaSparkContext sparkContext = new JavaSparkContext();
		
		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		int n = 100000 * slices;
		List<Integer> l = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			l.add(i);
		}
		
		JavaRDD<Integer> dataSet = sparkContext.parallelize(l, slices);
		int count = dataSet.map(new Function<Integer, Integer>() {
			private static final long serialVersionUID = -5146745675334154169L;

			@Override
			public Integer call(Integer v1) throws Exception {
				double x = Math.random() * 2 - 1;
				double y = Math.random() * 2 - 1;
				return (x * x + y * y < 1) ? 1 : 0;
			}
			
		}).reduce(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = -5862061911552056025L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				
				return v1 + v2;
			}
			
		});
		
		System.out.println("Pi is roughly " + 4.0 * count / n);
		
		sparkContext.stop();
		sparkContext.close();
	}
}
