package me.paulbares.query;

import me.paulbares.query.database.QueryRewriter;
import me.paulbares.store.Field;

import java.util.Objects;
import java.util.function.Function;

public class BinaryOperationMeasure implements Measure {

  public String alias;
  public BinaryOperator operator;
  public Measure leftOperand;
  public Measure rightOperand;

  /**
   * For jackson.
   */
  public BinaryOperationMeasure() {
  }

  public BinaryOperationMeasure(String alias,
                                BinaryOperator binaryOperator,
                                Measure leftOperand,
                                Measure rightOperand) {
    this.alias = alias;
    this.operator = binaryOperator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  @Override
  public String sqlExpression(Function<String, Field> fieldProvider, QueryRewriter queryRewriter) {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return quoteExpression(this.leftOperand) + " " + this.operator.infix + " " + quoteExpression(this.rightOperand);
  }

  private static String quoteExpression(Measure m) {
    if (m.alias() != null) {
      return m.alias();
    }
    String expression = m.expression();
    if (!(m instanceof AggregatedMeasure)) {
      return '(' + expression + ')';
    } else {
      return expression;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BinaryOperationMeasure that = (BinaryOperationMeasure) o;
    return Objects.equals(this.alias, that.alias) && this.operator == that.operator && Objects.equals(this.leftOperand, that.leftOperand) && Objects.equals(this.rightOperand, that.rightOperand);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.alias, this.operator, this.leftOperand, this.rightOperand);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() +
            "{" +
            "alias='" + this.alias + '\'' +
            ", operator=" + this.operator +
            ", leftOperand=" + this.leftOperand +
            ", rightOperand=" + this.rightOperand +
            '}';
  }
}
