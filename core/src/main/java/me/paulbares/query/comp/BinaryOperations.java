package me.paulbares.query.comp;

import me.paulbares.query.Operator;

import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BinaryOperations {

  public static final String ABS_DIFF = "absolute_difference";
  public static final String REL_DIFF = "relative_difference";

  public static BiFunction<Number, Number, Number> createComparisonBiFunction(String method, Class<?> dataType) {
    Class<? extends Number> outputDataType = getComparisonOutputType(method, dataType);
    return switch (method) {
      case ABS_DIFF ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() - b.longValue() : (a, b) -> a.doubleValue() - b.doubleValue();
      case REL_DIFF -> (a, b) -> (a.doubleValue() - b.doubleValue()) / b.doubleValue();
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  public static Class<? extends Number> getComparisonOutputType(String method, Class<?> dataType) {
    return switch (method) {
      case ABS_DIFF -> (Class<? extends Number>) dataType;
      case REL_DIFF -> double.class;
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  public static BiFunction<Number, Number, Number> createBiFunction(Operator operator,
                                                                    Class<?> leftDataType,
                                                                    Class<?> rightDataType) {
    Class<? extends Number> outputDataType = getOutputType(operator, leftDataType, rightDataType);
    return switch (operator) {
      case PLUS ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() + b.longValue() : (a, b) -> a.doubleValue() + b.doubleValue();
      case MINUS ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() - b.longValue() : (a, b) -> a.doubleValue() - b.doubleValue();
      case MULTIPLY ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() * b.longValue() : (a, b) -> a.doubleValue() * b.doubleValue();
      case DIVIDE -> (a, b) -> a.doubleValue() / b.doubleValue();
    };
  }

  public static Class<? extends Number> getOutputType(Operator operator, Class<?> leftDataType, Class<?> rightDataType) {
    Class<? extends Number> outputDataType = Stream.of(double.class, float.class, long.class, int.class)
            .filter(clazz -> clazz.equals(leftDataType) || clazz.equals(rightDataType))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("Types %s and %s are incompatible with the operator %s", leftDataType, rightDataType, operator)));
    if (outputDataType.equals(float.class)) {
      outputDataType = double.class;
    } else if (outputDataType.equals(int.class)) {
      outputDataType = long.class;
    }
    return outputDataType;
  }
}
