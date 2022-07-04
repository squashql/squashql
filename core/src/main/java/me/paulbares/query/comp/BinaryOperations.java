package me.paulbares.query.comp;

import me.paulbares.query.BinaryOperationMeasure;

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

  public static Class<?> getOutputType(BinaryOperationMeasure.Operator operator, Class<?> leftType, Class<?> rightType) {
    return switch (operator) {
      case PLUS, MINUS, MULTIPLY -> {
        Class<?> result;
        if (leftType.equals(long.class) || leftType.equals(int.class)) {
          result = rightType;
        } else {
          result = Double.class;
        }
        yield result;
      }
      case DIVIDE -> Double.class;
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

  public static Object apply(BinaryOperationMeasure.Operator operator, Object left, Object right, Class<?> leftDataType, Class<?> rightDataType) {
    Class<?> outputType = getOutputType(operator, leftDataType, rightDataType);
    return switch (operator) {
      case PLUS -> outputType.cast(add((Number) left, (Number) right));
      case MINUS -> outputType.cast(minus((Number) left, (Number) right));
      case MULTIPLY -> outputType.cast(multiply((Number) left, (Number) right));
      case DIVIDE -> outputType.cast(divide((Number) left, (Number) right));
    };
  }

  public static <T extends Number> Number add(T a, T b) {
    return a.doubleValue() + b.doubleValue();
  }

  public static <T extends Number> Number minus(T a, T b) {
    return a.doubleValue() - b.doubleValue();
  }

  public static <T extends Number> Number multiply(T a, T b) {
    return a.doubleValue() * b.doubleValue();
  }

  public static <T extends Number> Number divide(T a, T b) {
    return a.doubleValue() / b.doubleValue();
  }
}
