package io.squashql.query.builder;

import io.squashql.query.ColumnSet;
import io.squashql.query.Field;
import io.squashql.query.Measure;

import java.util.Collections;
import java.util.List;

public interface HasCondition {

  default CanAddRollup select(List<? extends Field> columns, List<Measure> measures) {
    return select(columns, Collections.emptyList(), measures);
  }

  default CanAddRollup select_(List<ColumnSet> columnSets, List<Measure> measures) {
    return select(Collections.emptyList(), columnSets, measures);
  }

  CanAddRollup select(List<? extends Field> columns, List<ColumnSet> columnSets, List<Measure> measures);
}
