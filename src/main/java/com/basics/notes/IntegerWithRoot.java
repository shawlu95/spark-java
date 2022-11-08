package com.basics.notes;

public class IntegerWithRoot {
    private int orig;
    private double root;

    public IntegerWithRoot(int i) {
        this.orig = i;
        this.root = Math.sqrt(orig);
    }
}
