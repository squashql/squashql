package me.paulbares.query;

import java.util.Objects;

import static me.paulbares.query.sql.SQLTranslator.escape;

public class ExpressionMeasure implements Measure {

  public String alias;
  public String expression;

  /**
   * For jackson.
   */
  public ExpressionMeasure() {
  }

  public ExpressionMeasure(String alias, String expression) {
    this.alias = Objects.requireNonNull(alias);
    this.expression = Objects.requireNonNull(expression);
  }

  @Override
  public String sqlExpression() {
    return this.expression + " as " + escape(this.alias);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionMeasure that = (ExpressionMeasure) o;
    return alias.equals(that.alias) && expression.equals(that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    return "ExpressionMeasure{" +
            "alias='" + alias + '\'' +
            ", expression='" + expression + '\'' +
            '}';
  }
}
