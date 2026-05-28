package io.squashql.query.util;

import io.squashql.query.BinaryOperator;
import io.squashql.query.ComparisonMethod;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BinaryOperations {

  public static BiFunction<Number, Number, Number> createComparisonBiFunction(ComparisonMethod method, Class<?> dataType) {
    Class<? extends Number> outputDataType = getComparisonOutputType(method, dataType);
    boolean isBigDecimal = outputDataType.equals(BigDecimal.class);
    boolean isLong = outputDataType.equals(long.class) || outputDataType.equals(Long.class);
    return switch (method) {
      case ABSOLUTE_DIFFERENCE -> isBigDecimal ? BinaryOperations::minusAsBigDecimal
              : isLong ? BinaryOperations::minusAsLong
              : BinaryOperations::minusAsDouble;
      case RELATIVE_DIFFERENCE -> isBigDecimal ? relativeDifferenceAsBigDecimal() : relativeDifference();
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

  private static BiFunction<Number, Number, Number> relativeDifferenceAsBigDecimal() {
    return (a, b) -> {
      if (a == null || b == null) {
        return null;
      }
      BigDecimal ba = toBigDecimal(a);
      BigDecimal bb = toBigDecimal(b);
      return ba.subtract(bb).divide(bb, MathContext.DECIMAL128);
    };
  }

  public static Class<? extends Number> getComparisonOutputType(ComparisonMethod method, Class<?> dataType) {
    return switch (method) {
      case ABSOLUTE_DIFFERENCE -> (Class<? extends Number>) dataType;
      case RELATIVE_DIFFERENCE, DIVIDE -> dataType.equals(BigDecimal.class) ? BigDecimal.class : double.class;
    };
  }

  public static BiFunction<Number, Number, Number> createBiFunction(BinaryOperator binaryOperator,
                                                                    Class<?> leftDataType,
                                                                    Class<?> rightDataType) {
    Class<? extends Number> outputDataType = getOutputType(binaryOperator, leftDataType, rightDataType);
    final boolean isBigDecimal = outputDataType.equals(BigDecimal.class);
    final boolean isLong = outputDataType.equals(long.class) || outputDataType.equals(Long.class);
    return switch (binaryOperator) {
      case PLUS -> isBigDecimal ? BinaryOperations::plusAsBigDecimal
              : isLong ? BinaryOperations::plusAsLong
              : BinaryOperations::plusAsDouble;
      case MINUS -> isBigDecimal ? BinaryOperations::minusAsBigDecimal
              : isLong ? BinaryOperations::minusAsLong
              : BinaryOperations::minusAsDouble;
      case MULTIPLY -> isBigDecimal ? BinaryOperations::multiplyAsBigDecimal
              : isLong ? BinaryOperations::multiplyAsLong
              : BinaryOperations::multiplyAsDouble;
      case DIVIDE -> isBigDecimal ? BinaryOperations::divideAsBigDecimal : BinaryOperations::divideAsDouble;
      case RELATIVE_DIFFERENCE -> isBigDecimal ? relativeDifferenceAsBigDecimal() : relativeDifference();
    };
  }

  public static Class<? extends Number> getOutputType(BinaryOperator binaryOperator, Class<?> leftDataType, Class<?> rightDataType) {
    Class<? extends Number> outputDataType = Stream.of(
                    BigDecimal.class,
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

  // asBigDecimal

  public static BigDecimal plusAsBigDecimal(Number a, Number b) {
    if (a == null) {
      return b == null ? null : toBigDecimal(b);
    }
    if (b == null) {
      return toBigDecimal(a);
    }
    return toBigDecimal(a).add(toBigDecimal(b));
  }

  public static BigDecimal minusAsBigDecimal(Number a, Number b) {
    if (a == null) {
      return b == null ? null : toBigDecimal(b);
    }
    if (b == null) {
      return toBigDecimal(a);
    }
    return toBigDecimal(a).subtract(toBigDecimal(b));
  }

  public static BigDecimal multiplyAsBigDecimal(Number a, Number b) {
    if (a == null || b == null) {
      return null;
    }
    return toBigDecimal(a).multiply(toBigDecimal(b));
  }

  public static BigDecimal divideAsBigDecimal(Number a, Number b) {
    if (a == null || b == null) {
      return null;
    }
    return toBigDecimal(a).divide(toBigDecimal(b), MathContext.DECIMAL128);
  }

  private static BigDecimal toBigDecimal(Number n) {
    return n instanceof BigDecimal bd ? bd : new BigDecimal(n.toString());
  }
}
