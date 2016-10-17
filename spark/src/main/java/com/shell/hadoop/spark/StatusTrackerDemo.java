package com.shell.hadoop.spark;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class StatusTrackerDemo {
	public static final String APP_NAME = "StatusTrackerDemo";
	
	public static final class IdenfityWithDelay<T> implements Function<T, T> {
		@Override
		public T call(T v1) throws Exception {
			Thread.sleep(2 * 1000);
			return v1;
		}
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		SparkSession spark = SparkSession.builder().appName(APP_NAME).getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).map(new IdenfityWithDelay<Integer>());
		
		JavaFutureAction<List<Integer>> jobFuture = rdd.collectAsync();
		while (!jobFuture.isDone()) {
			Thread.sleep(1000);
			List<Integer> jobIds = jobFuture.jobIds();
			if (jobIds.isEmpty()) {
				continue;
			}
			
			int currentJobId = jobIds.get(jobIds.size() - 1);
			SparkJobInfo jobInfo = jsc.statusTracker().getJobInfo(currentJobId);
			SparkStageInfo stageInfo = jsc.statusTracker().getStageInfo(jobInfo.stageIds()[0]);
			System.out.println(stageInfo.numTasks() + " tasks total: " + stageInfo.numActiveTasks() +
					" active, " + stageInfo.numCompletedTasks() + " complete");
		}
		
		System.out.println("Job results are: " + jobFuture.get());
		jsc.close();
		spark.stop();
	}
}
