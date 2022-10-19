package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.Measure;

import java.util.Collections;
import java.util.List;

public interface HasGroupBy {
  default void select(List<String> columns, List<Measure> measures) {
    select(columns, Collections.emptyList(), measures);
  }

  void select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures);
}
