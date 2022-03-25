package me.paulbares.query;

import java.util.Objects;

public class UnresolvedExpressionMeasure implements Measure {

  public String alias;

  /**
   * For jackson.
   */
  public UnresolvedExpressionMeasure() {
  }

  public UnresolvedExpressionMeasure(String alias) {
    this.alias = Objects.requireNonNull(alias);
  }

  @Override
  public String sqlExpression() {
    throw new RuntimeException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnresolvedExpressionMeasure that = (UnresolvedExpressionMeasure) o;
    return Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias);
  }

  @Override
  public String toString() {
    return "UnresolvedExpressionMeasure{" +
            "alias='" + alias + '\'' +
            '}';
  }
}
