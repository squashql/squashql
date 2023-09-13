package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Objects;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ConstantField implements Field {

  public Object value;

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    return Objects.toString(this.value);
  }

  @Override
  public String name() {
    throw new IllegalStateException("Incorrect path of execution");
  }
}
