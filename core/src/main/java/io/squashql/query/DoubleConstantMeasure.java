package io.squashql.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

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
}
