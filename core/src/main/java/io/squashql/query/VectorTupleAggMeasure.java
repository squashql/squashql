package io.squashql.query;

import lombok.*;

import java.util.List;
import java.util.function.Function;

/**
 * Internal ONLY!
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class VectorTupleAggMeasure implements Measure {

  public String alias;
  public List<FieldAndAggFunc> fieldToAggregateAndAggFunc;
  public Field vectorAxis;
  public Function<List<Object>, Object> transformer;
  @With
  public String expression;

  public VectorTupleAggMeasure(String alias,
                               List<FieldAndAggFunc> fieldToAggregateAndAggFunc,
                               Field vectorAxis,
                               Function<List<Object>, Object> transformer) {
    this.alias = alias;
    this.fieldToAggregateAndAggFunc = fieldToAggregateAndAggFunc;
    this.vectorAxis = vectorAxis;
    this.transformer = transformer;
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }
}
