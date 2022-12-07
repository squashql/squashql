package me.paulbares.query.database;

public interface QueryRewriter {

  default String tableName(String table) {
    return table;
  }

  default String measureAlias(String alias) {
    return alias;
  }

  boolean doesSupportPartialRollup();
}
