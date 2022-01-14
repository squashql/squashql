package me.paulbares;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

public record Field(String name, Class<?> type) {

  public Column col() {
    return functions.col(name);
  }
}
