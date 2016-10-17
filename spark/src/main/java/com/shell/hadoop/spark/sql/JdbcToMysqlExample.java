package com.shell.hadoop.spark.sql;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class JdbcToMysqlExample {
	public static void main(String[] args) {
		
		SparkSession sparkSession = SparkSession.builder().appName("JdbcToMysqlExample").getOrCreate();
		Dataset<Row> jdbcDF = sparkSession
								.read()
								.format("jdbc")
								.option("driver", "com.mysql.jdbc.Driver")
								.option("url", "jdbc:mysql://192.168.16.47:3306/ar?useUnicode=true&characterEncoding=utf8")
								.option("dbtable", "books")    // 必要属性
								.option("user", "root")
								.option("password", "root")
								.load();
//		Dataset<Row> jdbcDF = sparkSession.read().format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", "jdbc:mysql://192.168.16.47:3306/ar?useUnicode=true&characterEncoding=utf8").option("user", "root").option("password", "root").load();
		
//		Dataset<Row> booksDF = sparkSession.sql("select * from books where id < 100");
//		booksDF.show();
		
		jdbcDF.groupBy("category_id").count().show();
		
		Dataset<Row> filterDF = jdbcDF.filter(col("id").lt(100));
		
		Dataset<Book> books = filterDF.map(new MapFunction<Row, Book>() {

			@Override
			public Book call(Row row) throws Exception {
				Integer id = row.<Integer>getAs("id");
				String name = row.<String>getAs("name");
				String xmlUrl = row.<String>getAs("xml_url");
				
				Book book = new Book();
				book.setId(id);
				book.setName(name);
				book.setXmlUrl(xmlUrl);
				return book;
			}
			
		}, Encoders.bean(Book.class));
		
		books.show();
		
//		Dataset<Row> bookContents = sparkSession.sql("select * from book_contents");
//		bookContents.show();
		
		
	}
}
