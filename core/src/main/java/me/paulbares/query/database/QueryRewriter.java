package me.paulbares.query.database;

public interface QueryRewriter {

  default String fieldName(String field) {
    return field;
  }

  default String tableName(String table) {
    return table;
  }

  default String measureAlias(String alias) {
    return alias;
  }

  default String rollup(String rollup) {
    return rollup;
  }

  default String groupingAlias(String field) {
    return String.format("___grouping___%s___", field);
  }

  boolean doesSupportPartialRollup();
}
