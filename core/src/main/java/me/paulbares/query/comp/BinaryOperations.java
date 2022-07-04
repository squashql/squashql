package me.paulbares.query.comp;

import me.paulbares.query.Operator;

import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BinaryOperations {

  public static final String ABS_DIFF = "absolute_difference";
  public static final String REL_DIFF = "relative_difference";

  public static Object compare(String method, Object leftValue, Object rightValue, Class<?> leftDataType, Class<?> rightDataType) {
    return switch (method) {
      case ABS_DIFF -> computeAbsoluteDiff(leftValue, rightValue, leftDataType, rightDataType);
      case REL_DIFF -> computeRelativeDiff(leftValue, rightValue, leftDataType, rightDataType);
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  public static Class<?> getOutputType(String method, Class<?> dataType) {
    return switch (method) {
      case ABS_DIFF -> dataType;
      case REL_DIFF -> double.class;
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  private static double computeRelativeDiff(Object left, Object right, Class<?> leftDataType, Class<?> rightDataType) {
    if (leftDataType.equals(Double.class) || leftDataType.equals(double.class)) {
      return (((double) left) - ((double) right)) / ((double) right);
    } else if (leftDataType.equals(Float.class) || leftDataType.equals(float.class)) {
      return (((float) left) - ((float) right)) / ((float) right);
    } else if (leftDataType.equals(Integer.class) || leftDataType.equals(int.class)) {
      return (double) (((int) left) - ((int) right)) / ((long) right);
    } else if (leftDataType.equals(Long.class) || leftDataType.equals(long.class)) {
      return (double) (((long) left) - ((long) right)) / ((long) right);
    } else {
      throw new RuntimeException("Unsupported type " + leftDataType);
    }
  }

  private static Object computeAbsoluteDiff(Object left, Object right, Class<?> leftDataType, Class<?> rightDataType) {
    if (leftDataType.equals(Double.class) || leftDataType.equals(double.class)) {
      return ((double) left) - ((double) right);
    } else if (leftDataType.equals(Float.class) || leftDataType.equals(float.class)) {
      return ((float) left) - ((float) right);
    } else if (leftDataType.equals(Integer.class) || leftDataType.equals(int.class)) {
      return ((int) left) - ((int) right);
    } else if (leftDataType.equals(Long.class) || leftDataType.equals(long.class)) {
      return ((long) left) - ((long) right);
    } else {
      throw new RuntimeException("Unsupported type " + leftDataType);
    }
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
