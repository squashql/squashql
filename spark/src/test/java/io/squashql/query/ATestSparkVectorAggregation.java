package io.squashql.query;

import io.squashql.TestClass;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.List;

@TestClass
public abstract class ATestSparkVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    return JavaConverters.seqAsJavaList(((WrappedArray.ofRef) actualVector).seq());
  }
}
