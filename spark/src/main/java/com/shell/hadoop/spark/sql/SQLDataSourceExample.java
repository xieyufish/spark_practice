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
	
	/**
	 * 基本数据源加载
	 * @param spark
	 */
	private static void runBasicDataSourceExample(SparkSession spark) {
		
		// Spark SQL的默认的加载数据源格式为: parquet, 所以在加载parquet格式文件时可以不显示指定格式
		Dataset<Row> usersDF = spark.read().load("examples/src/main/resources/users.parquet");
		usersDF.select("name", "favorite_color").write().save("namesAndFavColors.parquet");
		
		// 加载非parquet格式文件时, 要显示指定文件的格式
		// spark 支持的简写格式有: parquet, json, jdbc等
		// spark也可以在多种格式之间互相转换
		Dataset<Row> peopleDF = spark.read().format("json").load("examples/src/main/resources/people.json");
		peopleDF.select("name", "age").write().format("parquet").save("nameAndAges.parquet");
		
		// spark可以直接从文件查询
		Dataset<Row> sqlDF = spark.sql("select * from parquet.`examples/src/main/resources/users.parquet`");
		sqlDF.show();
	}
	
	/**
	 * 针对parquet数据格式的操作
	 * @param spark
	 */
	private static void runBasicParquetExample(SparkSession spark) {
		// 将其他格式的输入转换为parquet格式保存
		Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");
		peopleDF.write().parquet("people.parquet");
		
		// 可以直接通过parquet方法从parquet格式的文件读取内容, 类似的json和jdbc都可以使用相同的方式
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");
		
		// 接下来的操作跟普通操作一样的
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
	
	/**
	 * 针对分区表的自动搜索查找功能
	 * @param spark
	 */
	private static void runParquetSchemaMergingExample(SparkSession spark) {
		List<Square> squares = new ArrayList<>();
		for (int value = 1; value <= 5; value++ ) {
			Square square = new Square();
			square.setValue(value);
			square.setSquare(value * value);
			squares.add(square);
		}
		
		// 以key作为分区列, 创建一个分区表,并存入分区目录中
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
		
		// 合并分区表, 必须要制定mergerSchema属性为true, 否则不能合并, 也可以通过spark配置文件设置这个属性值
		Dataset<Row> mergeDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
		mergeDF.printSchema();
	}
	
	/**
	 * json文件的读取
	 * @param spark
	 */
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
