package me.paulbares.query;

import java.util.function.BiFunction;

public enum ComparisonMethod {

  ABSOLUTE_DIFFERENCE((a, b) -> String.format("%s - %s", a, b)),
  RELATIVE_DIFFERENCE((a, b) -> String.format("(%s - %s) / %s", a, b, b)),
  DIVIDE((a, b) -> String.format("%s / %s", a, b));

  public final BiFunction<String, String, String> expressionGenerator;

  ComparisonMethod(BiFunction<String, String, String> expressionGenerator) {
    this.expressionGenerator = expressionGenerator;
  }
}
