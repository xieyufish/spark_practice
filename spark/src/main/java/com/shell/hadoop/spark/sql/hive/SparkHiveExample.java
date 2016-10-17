package com.shell.hadoop.spark.sql.hive;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkHiveExample {

	public static class Record implements Serializable {
		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

	}
	
	public static void main(String[] args) {
		String warehouseLocation = "file:" + System.getProperty("user.dir") + "spark-warehouse";
		
		SparkSession spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate();
		
		spark.sql("create table if not exists src(key INT, value STRING)");
		spark.sql("load data local inpath 'examples/src/main/resources/kv1.txt' into table src");
		
		spark.sql("select * from src").show();
		
		spark.sql("select count(*) from src").show();
		
		Dataset<Row> sqlDF = spark.sql("select key, value from src where key < 10 order by key");
		Dataset<String> stringsDS = sqlDF.map(new MapFunction<Row, String>() {

			@Override
			public String call(Row value) throws Exception {
				return "Key: " + value.get(0) + ", Value: " + value.get(1);
			}
			
		}, Encoders.STRING());
		stringsDS.show();
		
		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
			Record record = new Record();
			record.setKey(key);
			record.setValue("val_" + key);
			records.add(record);
		}
		
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");
		
		spark.sql("select * from records r join src s on r.key = s.key").show();
		
		spark.stop();
	}
}
