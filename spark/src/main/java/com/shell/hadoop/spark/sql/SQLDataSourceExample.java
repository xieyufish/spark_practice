package com.shell.hadoop.spark.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SQLDataSourceExample {

	public static class Square implements Serializable {
		private int value;
		private int square;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getSquare() {
			return square;
		}

		public void setSquare(int square) {
			this.square = square;
		}

	}

	public static class Cube implements Serializable {
		private int value;
		private int cube;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getCube() {
			return cube;
		}

		public void setCube(int cube) {
			this.cube = cube;
		}

	}
	
	private static void runBasicDataSourceExample(SparkSession spark) {
		
		Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
		usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
		
		Dataset<Row> peopleDF = spark.read().format("json").load("examples/src/main/resources/people.json");
		peopleDF.select("name", "age").write().format("parquet").save("nameAndAges.parquet");
		
		Dataset<Row> sqlDF = spark.sql("select * from parquet.`examples/src/main/resources/users.parquet`");
		sqlDF.show();
	}
	
	private static void runBasicParquetExample(SparkSession spark) {
		Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");
		peopleDF.write().parquet("people.parquet");
		
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("select name from parquetFile where age between 13 and 19");
		Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				return "Name: " + value.getString(0);
			}
			
		}, Encoders.STRING());
		
		namesDS.show();
	}
	
	private static void runParquetSchemaMergingExample(SparkSession spark) {
		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <= 5; value++ ) {
			Square square = new Square();
			square.setValue(value);
			square.setSquare(value * value);
			squares.add(square);
		}
		
		Dataset<Row> squareDF = spark.createDataFrame(squares, Square.class);
		squareDF.write().parquet("data/test_table/key=1");
		
		List<Cube> cubes = new ArrayList<>();
		for (int value = 6; value <= 10; value++) {
			Cube cube = new Cube();
			cube.setValue(value);
			cube.setCube(value * value * value);
			cubes.add(cube);
		}
		
		Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
		cubesDF.write().parquet("data/test_table/key=2");
		
		Dataset<Row> mergeDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
		mergeDF.printSchema();
	}
	
	private static void runJsonDatasetExample(SparkSession spark) {
		Dataset<Row> people = spark.read().json("examples/src/main/resources/people.json");
		people.printSchema();
		
		people.createOrReplaceTempView("people");
		
		Dataset<Row> namesDF = spark.sql("select name from people where age between 13 and 19");
		namesDF.show();
		
	}
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("SQL Data Sources Example").config("spark.some.config.option", "some-value").getOrCreate();
		
		runBasicDataSourceExample(spark);
		runBasicParquetExample(spark);
		runParquetSchemaMergingExample(spark);
		runJsonDatasetExample(spark);
		
		spark.stop();
		
	}
}
