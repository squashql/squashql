package io.squashql.query;

import lombok.*;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.Field;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public abstract class ConstantMeasure<T> implements Measure {

  public T value;
  @With
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
}
