package me.paulbares.query.builder;

import java.util.List;

public interface CanAddRollup extends HasOrderBy, CanAddOrderBy {

  HasSelectAndRollup rollup(String... columns);

  default HasSelectAndRollup rollup(List<String> columns) {
    return rollup(columns.toArray(new String[0]));
  }
}
