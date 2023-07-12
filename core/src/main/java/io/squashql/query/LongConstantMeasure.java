package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.store.Field;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

import java.util.function.Function;

@ToString
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor // For Jackson
public class LongConstantMeasure extends ConstantMeasure<Long> {

  public LongConstantMeasure(@NonNull Long value) {
    super(value);
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    return Long.toString(this.value);
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
  public ConstantMeasure<Long> withExpression(String expression) {
    LongConstantMeasure measure = new LongConstantMeasure(this.value);
    measure.expression = expression;
    return measure;
  }
}
