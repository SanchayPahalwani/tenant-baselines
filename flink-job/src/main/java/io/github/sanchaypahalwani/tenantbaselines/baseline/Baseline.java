package io.github.sanchaypahalwani.tenantbaselines.baseline;

import java.io.Serializable;

/** A bucket's typical value and spread; scaledMad is already 1.4826 times the MAD. */
public record Baseline(double median, double scaledMad) implements Serializable {}
