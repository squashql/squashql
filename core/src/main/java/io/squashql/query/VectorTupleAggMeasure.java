package io.squashql.query;

import lombok.*;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;

/**
 * Internal ONLY!
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class VectorTupleAggMeasure implements Measure {

  public String alias;
  public List<Pair<Field, String>> fieldToAggregateAndAggFunc;
  public Field vectorAxis;
  @With
  public String expression;

  public VectorTupleAggMeasure(String alias, List<Pair<Field, String>> fieldToAggregateAndAggFunc, Field vectorAxis) {
    this.alias = alias;
    this.fieldToAggregateAndAggFunc = fieldToAggregateAndAggFunc;
    this.vectorAxis = vectorAxis;
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
