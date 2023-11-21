package io.squashql.query;

import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class ExpressionMeasure implements BasicMeasure {

  public String alias;
  @With
  public String expression;

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }

}
