package io.squashql.query;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.function.Function;

import static io.squashql.query.database.SqlUtils.singleOperandFunctionName;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class FunctionField implements Field {

  public String function;
  public Field field;

  @Override
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    TypedField typedField = fieldProvider.apply(this.field);
    return queryRewriter.functionExpression(new FunctionTypedField((TableTypedField) typedField, this.function));
  }

  @Override
  public String name() {
    return singleOperandFunctionName(this.function, this.field.name());
  }
}
