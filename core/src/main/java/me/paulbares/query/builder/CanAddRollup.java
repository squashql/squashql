package me.paulbares.query.builder;

import java.util.List;

public interface CanAddRollup extends HasOrderBy, CanAddOrderBy{

  HasSelect rollup(String... columns);

  default HasSelect rollup(List<String> columns) {
    return rollup(columns.toArray(new String[0]));
  }
}
