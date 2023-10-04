package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

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
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return Double.toString(this.value);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + "{");
    sb.append("value=").append(this.value);
    sb.append(", expression='").append(this.expression).append('\'');
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
