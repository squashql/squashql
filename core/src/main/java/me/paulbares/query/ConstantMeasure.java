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
public abstract class ConstantMeasure<T> implements Measure {

  public T value;
  public String expression;

  public ConstantMeasure(@NonNull T value) {
    this.value = value;
  }

  public T getValue() {
    return this.value;
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
