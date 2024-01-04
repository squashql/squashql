package io.squashql.query;

import io.squashql.TestClass;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@TestClass
public abstract class ATestSnowflakeVectorAggregation extends ATestVectorAggregation {

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    // It is a string that needs to be parsed, see https://github.com/snowflakedb/snowflake-jdbc/issues/462
    String[] split = ((String) actualVector).replace("\n", "")
            .replace("[", "")
            .replace("]", "")
            .split(",");
    List<Number> r = new ArrayList<>();
    for (String s : split) {
      String trim = s.trim();
      BigDecimal bigDecimal = new BigDecimal(trim);
      double v = bigDecimal.doubleValue();
      r.add(v);
    }
    return r;
  }
}
