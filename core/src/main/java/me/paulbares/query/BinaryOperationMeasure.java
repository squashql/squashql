package me.paulbares.query;

import java.util.Objects;

public class BinaryOperationMeasure implements Measure {

  public String alias;

  public Operator operator;

  public Measure leftOperand;
  public Measure rightOperand;

  /**
   * For jackson.
   */
  public BinaryOperationMeasure() {
  }

  public BinaryOperationMeasure(String alias,
                                Operator operator,
                                Measure leftOperand,
                                Measure rightOperand) {
    this.alias = alias;
    this.operator = operator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  @Override
  public String sqlExpression() {
    throw new IllegalStateException();
  }

  @Override
  public String alias() {
    return this.alias; // TODO return default name if null
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
            "alias='" + alias + '\'' +
            ", operator=" + operator +
            ", leftOperand=" + leftOperand +
            ", rightOperand=" + rightOperand +
            '}';
  }
}
