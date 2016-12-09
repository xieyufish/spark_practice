package com.shell.hadoop.spark.ml;

import java.util.Arrays;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PipelineExample {
	
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().appName("PipelineExample").getOrCreate();
		
		Dataset<Row> training = sparkSession.createDataFrame(Arrays.asList(
				new LabeledDocument(0l, "a b c d e spark", 1.0),
				new LabeledDocument(1l, "b d", 0.0),
				new LabeledDocument(2l, "spark f g h", 1.0),
				new LabeledDocument(3l, "hadoop mapreduce", 0.0)), LabeledDocument.class);
		
		Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
		HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
		LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01);
		
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, hashingTF, lr});
		
		PipelineModel model = pipeline.fit(training);
		
		Dataset<Row> test = sparkSession.createDataFrame(Arrays.asList(
				new Document(4l, "spark i j k"),
				new Document(5l, "l m n"),
				new Document(6l, "mapreduce spark"),
				new Document(7l, "apache hadoop")), Document.class);
		
		Dataset<Row> predictions = model.transform(test);
		predictions.show();
		/*for (Row r : predictions.select("id", "text", "probability", "prediction").collectAsList()) {
			System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2) + ", prediction=" + r.get(3));
		}*/
		
		sparkSession.stop();
		
	}
}
