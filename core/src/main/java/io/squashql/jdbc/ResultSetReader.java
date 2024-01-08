package io.squashql.jdbc;

import java.sql.ResultSet;
import java.util.List;

import static io.squashql.jdbc.JdbcQueryEngine.getTypeValue;

public interface ResultSetReader {

  default Object read(List<Class<?>> columnTypes, ResultSet tableResult, int index) {
    return getTypeValue(columnTypes, tableResult, index);
  }
}
