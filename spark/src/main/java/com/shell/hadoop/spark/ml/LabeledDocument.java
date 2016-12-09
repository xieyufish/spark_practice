package com.shell.hadoop.spark.ml;

import java.io.Serializable;

public class LabeledDocument extends Document implements Serializable {

	private double label;

	public LabeledDocument(long id, String text, double label) {
		super(id, text);
		this.label = label;
	}

	public double getLabel() {
		return label;
	}

	public void setLabel(double label) {
		this.label = label;
	}

}
