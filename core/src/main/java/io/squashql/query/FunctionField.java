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
@NoArgsConstructor
@AllArgsConstructor
public class FunctionField implements Field {

  private String store;
  private String expression;

  @Override
  public String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    return queryRewriter.selectDate(new io.squashql.type.FunctionField(this.store, this.expression));
  }

  @Override
  public String name() {
    return this.expression;
  }
}
