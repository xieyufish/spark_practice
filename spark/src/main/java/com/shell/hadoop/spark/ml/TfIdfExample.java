package com.shell.hadoop.spark.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TfIdfExample {
	
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().appName("TfIdfExample").getOrCreate();
		
		List<Row> data = Arrays.asList(
				RowFactory.create(0.0, "Hi I heard about Spark"),
				RowFactory.create(0.0, "I wish Java could use case classes"),
				RowFactory.create(1.0, "Logistic regression models are neat"));
		
		StructType schema = new StructType(new StructField[] {
				new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
		});
		
		Dataset<Row> sentenceData = sparkSession.createDataFrame(data, schema);
		
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(sentenceData);
		wordsData.show(false);
		
		int numFeatures = 20;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(numFeatures);
		
		Dataset<Row> featurizedData = hashingTF.transform(wordsData);
		featurizedData.show(false);
		
		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		for (Row r : rescaledData.select("features", "label").takeAsList(3)) {
			Vector features = r.getAs(0);
			Double label = r.getAs(1);
			System.out.println(features);
			System.out.println(label);
		}
		
		sparkSession.stop();
	}
}
