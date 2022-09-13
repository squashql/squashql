package me.paulbares.query;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class ConstantMeasure implements Measure {

  public Object value;
  public Class<?> type;
  public String expression;

  public ConstantMeasure(@NonNull Object value, @NonNull Class<?> type) {
    this.value = value;
    this.type = type;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return "constant(" + this.value + ")";
  }

  @Override
  public String expression() {
    return this.expression;
  }

  @Override
  public void setExpression(String expression) {
    this.expression = expression;
  }
}
