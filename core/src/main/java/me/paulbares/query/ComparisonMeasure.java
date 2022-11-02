package me.paulbares.query;

/**
 * Marker interface.
 */
public interface ComparisonMeasure {

  ComparisonMethod getComparisonMethod();

  Measure getMeasure();
}
