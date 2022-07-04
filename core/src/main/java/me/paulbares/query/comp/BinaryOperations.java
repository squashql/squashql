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
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() - b.longValue() : (a, b) -> a.doubleValue() - b.doubleValue();
      case RELATIVE_DIFFERENCE -> (a, b) -> (a.doubleValue() - b.doubleValue()) / b.doubleValue();
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  public static Class<? extends Number> getComparisonOutputType(ComparisonMethod method, Class<?> dataType) {
    return switch (method) {
      case ABSOLUTE_DIFFERENCE -> (Class<? extends Number>) dataType;
      case RELATIVE_DIFFERENCE -> double.class;
    };
  }

  public static BiFunction<Number, Number, Number> createBiFunction(BinaryOperator binaryOperator,
                                                                    Class<?> leftDataType,
                                                                    Class<?> rightDataType) {
    Class<? extends Number> outputDataType = getOutputType(binaryOperator, leftDataType, rightDataType);
    return switch (binaryOperator) {
      case PLUS ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() + b.longValue() : (a, b) -> a.doubleValue() + b.doubleValue();
      case MINUS ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() - b.longValue() : (a, b) -> a.doubleValue() - b.doubleValue();
      case MULTIPLY ->
              outputDataType.equals(long.class) ? (a, b) -> a.longValue() * b.longValue() : (a, b) -> a.doubleValue() * b.doubleValue();
      case DIVIDE -> (a, b) -> a.doubleValue() / b.doubleValue();
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
}
