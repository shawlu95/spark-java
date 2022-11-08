package com.basics.ch5_tuples;

public class IntegerWithSquareRoot {

	private int originalNumber;
	private double squareRoot;
	
	public IntegerWithSquareRoot(int i) {
		this.originalNumber = i;
		this.squareRoot = Math.sqrt(originalNumber);
	}

}
