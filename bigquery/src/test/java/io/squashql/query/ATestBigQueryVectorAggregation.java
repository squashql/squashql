package io.squashql.query;

import com.google.cloud.bigquery.FieldValueList;
import io.squashql.TestClass;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@TestClass
public abstract class ATestBigQueryVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    List<Number> r = new ArrayList<>();
    ((FieldValueList) actualVector).forEach(e -> r.add(new BigDecimal((String) e.getValue()).doubleValue()));
    return r;
  }
}
