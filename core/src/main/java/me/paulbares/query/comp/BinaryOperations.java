package me.paulbares.query.comp;

import me.paulbares.query.BinaryOperator;
import me.paulbares.query.ComparisonMethod;

import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BinaryOperations {

  public static BiFunction<Number, Number, Number> createComparisonBiFunction(ComparisonMethod method, Class<?> dataType) {
    Class<? extends Number> outputDataType = getComparisonOutputType(method, dataType);
    return switch (method) {
      case ABSOLUTE_DIFFERENCE ->
              outputDataType.equals(long.class) ? BinaryOperations::minusAsLong : BinaryOperations::minusAsDouble;
      case RELATIVE_DIFFERENCE -> (a, b) -> {
        Double diff = BinaryOperations.minusAsDouble(a, b);
        if (diff == null || b == null) {
          return null;
        } else {
          return (a.doubleValue() - b.doubleValue()) / b.doubleValue();
        }
      };
      case DIVIDE -> createBiFunction(BinaryOperator.DIVIDE, dataType, dataType);
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
    return switch (binaryOperator) {
      case PLUS -> outputDataType.equals(long.class) ? BinaryOperations::plusAsLong : BinaryOperations::plusAsDouble;
      case MINUS -> outputDataType.equals(long.class) ? BinaryOperations::minusAsLong : BinaryOperations::minusAsDouble;
      case MULTIPLY ->
              outputDataType.equals(long.class) ? BinaryOperations::multiplyAsLong : BinaryOperations::multiplyAsDouble;
      case DIVIDE -> BinaryOperations::divideAsDouble;
    };
  }

  public static Class<? extends Number> getOutputType(BinaryOperator binaryOperator, Class<?> leftDataType, Class<?> rightDataType) {
    Class<? extends Number> outputDataType = Stream.of(double.class, float.class, long.class, int.class)
            .filter(clazz -> clazz.equals(leftDataType) || clazz.equals(rightDataType))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("Types %s and %s are incompatible with the operator %s", leftDataType, rightDataType, binaryOperator)));
    if (outputDataType.equals(float.class)) {
      outputDataType = double.class;
    } else if (outputDataType.equals(int.class)) {
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
      return a == null ? null : a.longValue();
    }
    return a.longValue() + b.longValue();
  }

  public static Long minusAsLong(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.longValue();
    }
    if (b == null) {
      return a == null ? null : a.longValue();
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
      return a == null ? null : a.doubleValue();
    }
    return a.doubleValue() + b.doubleValue();
  }

  public static Double minusAsDouble(Number a, Number b) {
    if (a == null) {
      return b == null ? null : b.doubleValue();
    }
    if (b == null) {
      return a == null ? null : a.doubleValue();
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
