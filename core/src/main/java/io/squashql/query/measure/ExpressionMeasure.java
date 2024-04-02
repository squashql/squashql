package io.squashql.query.measure;

import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ExpressionMeasure implements BasicMeasure {

  public String alias;
  @With
  public String expression;

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
