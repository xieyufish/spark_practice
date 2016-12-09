package com.shell.hadoop.spark.ml;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ALSExample {
	
	public static class Rating implements Serializable {
		private int userId;
		private int movieId;
		private float rating;
		private long timestamp;
		
		public Rating() {
			// TODO Auto-generated constructor stub
		}
		
		public Rating(int userId, int movieId, float rating, long timestamp) {
			this.userId = userId;
			this.movieId = movieId;
			this.rating = rating;
			this.timestamp = timestamp;
		}

		public int getUserId() {
			return userId;
		}

		public void setUserId(int userId) {
			this.userId = userId;
		}

		public int getMovieId() {
			return movieId;
		}

		public void setMovieId(int movieId) {
			this.movieId = movieId;
		}

		public float getRating() {
			return rating;
		}

		public void setRating(float rating) {
			this.rating = rating;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		
		public static Rating parseRating(String str) {
			String[] fields = str.split("::");
			if (fields.length != 4) {
				throw new IllegalArgumentException("Each line must contain 4 fields");
			}
			
			int userId = Integer.parseInt(fields[0]);
			int movieId = Integer.parseInt(fields[1]);
			float rating = Float.parseFloat(fields[2]);
			long timestamp = Long.parseLong(fields[3]);
			
			return new Rating(userId, movieId, rating, timestamp);
		}
		
	}
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().appName("ALSExample").getOrCreate();
		
		JavaRDD<Rating> ratingsRDD = sparkSession.read().textFile("file:/usr/local/spark-2.0/data/mllib/als/sample_movielens_ratings.txt").javaRDD()
				.map(new Function<String, Rating>() {
					
					@Override
					public Rating call(String v1) throws Exception {
						return Rating.parseRating(v1);
					}
					
				});
		
		Dataset<Row> ratings = sparkSession.createDataFrame(ratingsRDD, Rating.class);
		Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
		Dataset<Row> training = splits[0];
		Dataset<Row> test = splits[1];
		
		ALS als = new ALS()    // 这是一个estimator
				.setMaxIter(5)
				.setRegParam(0.01)
				.setUserCol("userId")
				.setItemCol("movieId")
				.setRatingCol("rating");
		ALSModel model = als.fit(training);  // 由estimator产生一个transformer
		
		Dataset<Row> predictions = model.transform(test);
		predictions.show(true);
		
		RegressionEvaluator evaluator = new RegressionEvaluator()
				.setMetricName("rmse")
				.setLabelCol("rating")
				.setPredictionCol("prediction");
		
		Double rmse = evaluator.evaluate(predictions);
		System.out.println("Root-mean-square error = " + rmse);
		
		sparkSession.stop();
		
	}
}
