package me.paulbares.query.agg;

import java.util.List;

public interface Aggregator {

  void aggregate(int rowIndex, List<Object> values);
}
