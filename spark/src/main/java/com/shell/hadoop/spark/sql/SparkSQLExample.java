package com.shell.hadoop.spark.sql;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQLExample {

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

	}
	
	private static void runBasicDataFrameExample(SparkSession spark) {
		Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");
		df.show();
		df.printSchema();
		df.select("name").show();
		df.select(col("name"), col("age").plus(1)).show();
		df.filter(col("age").gt(21)).show();
		df.groupBy("age").count().show();
		df.createOrReplaceTempView("people");
		
		Dataset<Row> sqlDF = spark.sql("select * from people");
		sqlDF.show();
	}
	
	private static void runDatasetCreationExample(SparkSession spark) {
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();
		
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {

			@Override
			public Integer call(Integer value) throws Exception {
				return value + 1;
			}
			
		}, integerEncoder);
		transformedDS.collect();
		
		String path = "examples/src/main/resources/people.json";
		Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
		peopleDS.show();
	}
	
	private static void runInferSchemaExample(SparkSession spark) {
		JavaRDD<Person> peopleRDD = spark.read().textFile("examples/src/main/resources/people.txt").javaRDD().map(new Function<String, Person>() {

			@Override
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));
				return person;
			}
			
		});
		
		Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
		peopleDF.createOrReplaceTempView("people");
		
		Dataset<Row> teenagerDF = spark.sql("select name from people where age between 13 and 19");
		Encoder<String> stringEncoder = Encoders.STRING();
		Dataset<String> teenagerNamesByIndexDF = teenagerDF.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				return "Name: " + value.<String>getAs("name");
			}
			
		}, stringEncoder);
		teenagerNamesByIndexDF.show();
		
		Dataset<String> teenagerNamesByFieldDF = teenagerDF.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				return "Name: " + value.<String>getAs("name");
			}
			
		}, stringEncoder);
		teenagerNamesByFieldDF.show();
		
	}
	
	private static void runProgrammaticSchemaExample(SparkSession spark) {
		JavaRDD<String> peopleRDD = spark.sparkContext().textFile("examples/src/main/resources/people.txt", 1).toJavaRDD();
		
		String schemaString = "name age";
		
		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}
		StructType schema = DataTypes.createStructType(fields);
		JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {

			@Override
			public Row call(String v1) throws Exception {
				String[] attributes = v1.split(",");
				return RowFactory.create(attributes[0], attributes[1].trim());
			}
			
		});
		
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
		peopleDataFrame.createOrReplaceTempView("people");
		
		Dataset<Row> results = spark.sql("select name from people");
		
		Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				return "Name: " + value.getString(0);
			}
			
		}, Encoders.STRING());
		namesDS.show();
	}
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Spark SQL Example").config("spark.some.config.option", "some-value").getOrCreate();
		
		runDatasetCreationExample(spark);
		runBasicDataFrameExample(spark);
		runInferSchemaExample(spark);
		runProgrammaticSchemaExample(spark);
		
		spark.stop();
	}
}
