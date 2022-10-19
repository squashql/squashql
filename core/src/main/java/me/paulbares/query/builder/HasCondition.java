package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;

import java.util.List;

public interface HasCondition {
  HasGroupBy groupBy(List<String> columns, ColumnSet... columnSets);
}
