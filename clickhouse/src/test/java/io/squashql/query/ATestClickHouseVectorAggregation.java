package io.squashql.query;

import io.squashql.TestClass;

import java.util.List;

@TestClass
public abstract class ATestClickHouseVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    return (List<Number>) actualVector;
  }
}
