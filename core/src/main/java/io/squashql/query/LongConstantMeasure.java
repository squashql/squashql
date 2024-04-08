package io.squashql.query;

import io.squashql.query.measure.visitor.MeasureVisitor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor // For Jackson
public class LongConstantMeasure extends ConstantMeasure<Long> {

  public LongConstantMeasure(@NonNull Long value) {
    super(value);
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
  public ConstantMeasure<Long> withExpression(String expression) {
    LongConstantMeasure measure = new LongConstantMeasure(this.value);
    measure.expression = expression;
    return measure;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
