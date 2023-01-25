package io.squashql.query;

import lombok.*;

@ToString
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor // For Jackson
public class DoubleConstantMeasure extends ConstantMeasure<Double> {

  public DoubleConstantMeasure(@NonNull Double value) {
    super(value);
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + "{");
    sb.append("value=").append(value);
    sb.append(", expression='").append(expression).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public ConstantMeasure<Double> withExpression(String expression) {
    DoubleConstantMeasure measure = new DoubleConstantMeasure(this.value);
    measure.expression = expression;
    return measure;
  }
}
