package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.database.SnowflakeQueryEngine;

import java.util.List;

@TestClass
public abstract class ATestSnowflakeVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    return SnowflakeQueryEngine.parseVectorString((String) actualVector).stream().map(e -> (Number) e).toList();
  }
}
