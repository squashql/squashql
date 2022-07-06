package me.paulbares.query;

import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

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
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
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
