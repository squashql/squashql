package io.squashql.query;

public enum BinaryOperator {
  PLUS("+"),
  MINUS("-"),
  MULTIPLY("*"),
  DIVIDE("/"),
  RELATIVE_DIFFERENCE(null);

  public final String infix;

  BinaryOperator(String infix) {
    this.infix = infix;
  }
}
