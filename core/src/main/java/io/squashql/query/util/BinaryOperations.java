package io.squashql.query.util;

import io.squashql.query.BinaryOperator;
import io.squashql.query.ComparisonMethod;

import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BinaryOperations {

  public static BiFunction<Number, Number, Number> createComparisonBiFunction(ComparisonMethod method, Class<?> dataType) {
    Class<? extends Number> outputDataType = getComparisonOutputType(method, dataType);
    boolean isLong = outputDataType.equals(long.class) || outputDataType.equals(Long.class);
    return switch (method) {
      case ABSOLUTE_DIFFERENCE -> isLong ? BinaryOperations::minusAsLong : BinaryOperations::minusAsDouble;
      case RELATIVE_DIFFERENCE -> relativeDifference();
      case DIVIDE -> createBiFunction(BinaryOperator.DIVIDE, dataType, dataType);
    };
  }

  private static BiFunction<Number, Number, Number> relativeDifference() {
    return (a, b) -> {
      Double diff = BinaryOperations.minusAsDouble(a, b);
      if (diff == null || b == null) {
        return null;
      } else {
        return (a.doubleValue() - b.doubleValue()) / b.doubleValue();
      }
    };
  }

  public static Class<? extends Number> getComparisonOutputType(ComparisonMethod method, Class<?> dataType) {
    return switch (method) {
      case ABSOLUTE_DIFFERENCE -> (Class<? extends Number>) dataType;
      case RELATIVE_DIFFERENCE, DIVIDE -> double.class;
    };
  }

  public static BiFunction<Number, Number, Number> createBiFunction(BinaryOperator binaryOperator,
                                                                    Class<?> leftDataType,
                                                                    Class<?> rightDataType) {
    Class<? extends Number> outputDataType = getOutputType(binaryOperator, leftDataType, rightDataType);
    final boolean isLong = outputDataType.equals(long.class) || outputDataType.equals(Long.class);
    return switch (binaryOperator) {
      case PLUS -> isLong ? BinaryOperations::plusAsLong : BinaryOperations::plusAsDouble;
      case MINUS -> isLong ? BinaryOperations::minusAsLong : BinaryOperations::minusAsDouble;
      case MULTIPLY -> isLong ? BinaryOperations::multiplyAsLong : BinaryOperations::multiplyAsDouble;
      case DIVIDE -> BinaryOperations::divideAsDouble;
      case RELATIVE_DIFFERENCE -> relativeDifference();
    };
  }

  public static Class<? extends Number> getOutputType(BinaryOperator binaryOperator, Class<?> leftDataType, Class<?> rightDataType) {
    Class<? extends Number> outputDataType = Stream.of(
                    double.class, Double.class,
                    float.class, Float.class,
                    long.class, Long.class,
                    int.class, Integer.class)
            .filter(clazz -> clazz.equals(leftDataType) || clazz.equals(rightDataType))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("Types %s and %s are incompatible with the operator %s", leftDataType, rightDataType, binaryOperator)));
    if (outputDataType.equals(float.class) || outputDataType.equals(Float.class)) {
      outputDataType = double.class;
    } else if (outputDataType.equals(int.class) || outputDataType.equals(Integer.class)) {
      outputDataType = long.class;
    }
    return outputDataType;
  }

  // asLong

  public static Long plusAsLong(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.longValue();
    }
    if (b == null) {
      return a.longValue();
    }
    return a.longValue() + b.longValue();
  }

  public static Long minusAsLong(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.longValue();
    }
    if (b == null) {
      return a.longValue();
    }
    return a.longValue() - b.longValue();
  }

  public static Long multiplyAsLong(Number a, Number b) {
    if (a == null || b == null) {
      return null;
    }
    return a.longValue() * b.longValue();
  }

  // asDouble

  public static Double plusAsDouble(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.doubleValue();
    }
    if (b == null) {
      return a.doubleValue();
    }
    return a.doubleValue() + b.doubleValue();
  }

  public static Double minusAsDouble(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.doubleValue();
    }
    if (b == null) {
      return a.doubleValue();
    }
    return a.doubleValue() - b.doubleValue();
  }

  public static Double multiplyAsDouble(Number a, Number b) {
    if (a == null || b == null) {
      return null;
    }
    return a.doubleValue() * b.doubleValue();
  }

  public static Double divideAsDouble(Number a, Number b) {
    if (a == null || b == null) {
      return null;
    }
    return a.doubleValue() / b.doubleValue();
  }
}
