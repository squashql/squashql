package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FunctionField implements Field {

  private String expression;

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public String name() {
    return this.expression;
  }
}
