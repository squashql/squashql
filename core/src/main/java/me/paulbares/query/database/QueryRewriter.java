package me.paulbares.query.database;

import me.paulbares.query.Measure;

public interface QueryRewriter {

  default String tableName(String table) {
    return table;
  }

  default String measureAlias(String alias, Measure measure) {
    return alias;
  }

  boolean doesSupportPartialRollup();
}
