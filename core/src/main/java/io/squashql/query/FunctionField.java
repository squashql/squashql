package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TypedField;

import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FunctionField implements Field {

  private Field expression;

  @Override
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    FunctionTypedField typedField = (FunctionTypedField) fieldProvider.apply(this.expression);
    return queryRewriter.functionExpression(typedField);
  }

  @Override
  public String name() {
    return this.expression.name();
  }
}
