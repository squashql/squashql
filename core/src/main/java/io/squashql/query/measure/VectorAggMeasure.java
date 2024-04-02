package io.squashql.query.measure;

import io.squashql.query.measure.visitor.MeasureVisitor;
import io.squashql.query.field.Field;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class VectorAggMeasure implements Measure {

  public String alias;
  public Field fieldToAggregate;
  public String aggregationFunction;
  public Field vectorAxis;
  @With
  public String expression;

  public VectorAggMeasure(String alias, Field fieldToAggregate, String aggregationFunction, Field vectorAxis) {
    this.alias = alias;
    this.fieldToAggregate = fieldToAggregate;
    this.aggregationFunction = aggregationFunction;
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

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
