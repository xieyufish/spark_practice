package com.shell.hadoop.spark.sql;

public class Book {
	private Integer id;
	private String name;
	private String xmlUrl;
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getXmlUrl() {
		return xmlUrl;
	}
	public void setXmlUrl(String xmlUrl) {
		this.xmlUrl = xmlUrl;
	}
	
	public String toString() {
		return "id: " + this.id + "\nname: " + this.name + "\nxmlUrl: " + this.xmlUrl;
	}
}
