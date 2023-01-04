package io.squashql.query;

public enum BinaryOperator {
  PLUS("+"),
  MINUS("-"),
  MULTIPLY("*"),
  DIVIDE("/");

  public final String infix;

  BinaryOperator(String infix) {
    this.infix = infix;
  }
}
