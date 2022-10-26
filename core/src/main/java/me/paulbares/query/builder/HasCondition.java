package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.Measure;

import java.util.Collections;
import java.util.List;

public interface HasCondition {
  
  default HasSelect select(List<String> columns, List<Measure> measures) {
    return select(columns, Collections.emptyList(), measures);
  }

  default HasSelect select_(List<ColumnSet> columnSets, List<Measure> measures) {
    return select(Collections.emptyList(), columnSets, measures);
  }

  HasSelect select(List<String> columns, List<ColumnSet> columnSets, List<Measure> measures);
}
