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

  public String alias;
  public Object value;

  public ConstantField(Object value) {
    this.value = value;
  }

  @Override
  public String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter) {
    return Objects.toString(this.value);
  }

  @Override
  public String name() {
    return Objects.toString(this.value);
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public Field as(String alias) {
    return new ConstantField(alias, this.value);
  }
}
