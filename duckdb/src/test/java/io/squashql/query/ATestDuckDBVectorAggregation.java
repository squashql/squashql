package io.squashql.query;

import io.squashql.TestClass;
import org.duckdb.DuckDBArray;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@TestClass
public abstract class ATestDuckDBVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    DuckDBArray v = (DuckDBArray) actualVector;
    try {
      Object[] a = (Object[]) v.getArray();
      List<Number> l = new ArrayList<>();
      for (int i = 0; i < a.length; i++) {
        l.add((Number) a[i]);
      }
      return l;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
