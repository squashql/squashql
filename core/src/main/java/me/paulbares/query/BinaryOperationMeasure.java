package me.paulbares.query;

public class BinaryOperationMeasure implements Measure {

  public enum Operator {
    PLUS, MINUS, MULTIPLY, DIVIDE;
  }

  public static String KEY = "binary_operation";

  public String type;

  public String alias;

  public Operator operator;

  public Measure leftOperand;
  public Measure rightOperand;

  /**
   * For jackson.
   */
  public BinaryOperationMeasure() {
    this.type = KEY;
  }

  public BinaryOperationMeasure(String alias,
                                Operator operator,
                                Measure leftOperand,
                                Measure rightOperand) {
    this.alias = alias;
    this.operator = operator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
    this.type = KEY;
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
  public String toString() {
    return getClass().getSimpleName() +
            "{" +
            "type='" + type + '\'' +
            ", alias='" + alias + '\'' +
            ", operator=" + operator +
            ", leftOperand=" + leftOperand +
            ", rightOperand=" + rightOperand +
            '}';
  }
}
