package io.squashql.query.util;

import io.squashql.query.BinaryOperator;
import io.squashql.query.ComparisonMethod;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.function.BiFunction;

public class TestBinaryOperationsBigDecimal {

  @Test
  void testGetOutputTypeAcceptsBigDecimal() {
    Assertions.assertThat(BinaryOperations.getOutputType(BinaryOperator.DIVIDE, BigDecimal.class, BigDecimal.class))
            .isEqualTo(BigDecimal.class);
  }

  @Test
  void testGetOutputTypeMixedBigDecimalAndLongPromotesToBigDecimal() {
    Assertions.assertThat(BinaryOperations.getOutputType(BinaryOperator.PLUS, BigDecimal.class, long.class))
            .isEqualTo(BigDecimal.class);
    Assertions.assertThat(BinaryOperations.getOutputType(BinaryOperator.MULTIPLY, double.class, BigDecimal.class))
            .isEqualTo(BigDecimal.class);
  }

  @Test
  void testGetComparisonOutputTypeBigDecimal() {
    Assertions.assertThat(BinaryOperations.getComparisonOutputType(ComparisonMethod.DIVIDE, BigDecimal.class))
            .isEqualTo(BigDecimal.class);
    Assertions.assertThat(BinaryOperations.getComparisonOutputType(ComparisonMethod.ABSOLUTE_DIFFERENCE, BigDecimal.class))
            .isEqualTo(BigDecimal.class);
    Assertions.assertThat(BinaryOperations.getComparisonOutputType(ComparisonMethod.RELATIVE_DIFFERENCE, BigDecimal.class))
            .isEqualTo(BigDecimal.class);
  }

  @Test
  void testDivideAsBigDecimalExact() {
    BiFunction<Number, Number, Number> divide = BinaryOperations.createBiFunction(BinaryOperator.DIVIDE, BigDecimal.class, BigDecimal.class);
    Number r = divide.apply(new BigDecimal("10"), new BigDecimal("4"));
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("2.5"))).isZero();
  }

  @Test
  void testDivideAsBigDecimalNonTerminating() {
    BiFunction<Number, Number, Number> divide = BinaryOperations.createBiFunction(BinaryOperator.DIVIDE, BigDecimal.class, BigDecimal.class);
    Number r = divide.apply(new BigDecimal("1"), new BigDecimal("3"));
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("1").divide(new BigDecimal("3"), MathContext.DECIMAL128))).isZero();
  }

  @Test
  void testPlusMinusMultiplyAsBigDecimal() {
    BiFunction<Number, Number, Number> plus = BinaryOperations.createBiFunction(BinaryOperator.PLUS, BigDecimal.class, BigDecimal.class);
    BiFunction<Number, Number, Number> minus = BinaryOperations.createBiFunction(BinaryOperator.MINUS, BigDecimal.class, BigDecimal.class);
    BiFunction<Number, Number, Number> multiply = BinaryOperations.createBiFunction(BinaryOperator.MULTIPLY, BigDecimal.class, BigDecimal.class);

    Assertions.assertThat(((BigDecimal) plus.apply(new BigDecimal("1.5"), new BigDecimal("2.25"))).compareTo(new BigDecimal("3.75"))).isZero();
    Assertions.assertThat(((BigDecimal) minus.apply(new BigDecimal("10"), new BigDecimal("3"))).compareTo(new BigDecimal("7"))).isZero();
    Assertions.assertThat(((BigDecimal) multiply.apply(new BigDecimal("2.5"), new BigDecimal("4"))).compareTo(new BigDecimal("10"))).isZero();
  }

  @Test
  void testMixedBigDecimalAndDoublePreservesBigDecimal() {
    BiFunction<Number, Number, Number> divide = BinaryOperations.createBiFunction(BinaryOperator.DIVIDE, BigDecimal.class, double.class);
    Number r = divide.apply(new BigDecimal("10"), 4d);
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("2.5"))).isZero();
  }

  @Test
  void testNullHandlingMirrorsDoublePath() {
    BiFunction<Number, Number, Number> plus = BinaryOperations.createBiFunction(BinaryOperator.PLUS, BigDecimal.class, BigDecimal.class);
    BiFunction<Number, Number, Number> minus = BinaryOperations.createBiFunction(BinaryOperator.MINUS, BigDecimal.class, BigDecimal.class);
    BiFunction<Number, Number, Number> multiply = BinaryOperations.createBiFunction(BinaryOperator.MULTIPLY, BigDecimal.class, BigDecimal.class);
    BiFunction<Number, Number, Number> divide = BinaryOperations.createBiFunction(BinaryOperator.DIVIDE, BigDecimal.class, BigDecimal.class);

    Assertions.assertThat(((BigDecimal) plus.apply(null, new BigDecimal("2"))).compareTo(new BigDecimal("2"))).isZero();
    Assertions.assertThat(((BigDecimal) plus.apply(new BigDecimal("3"), null)).compareTo(new BigDecimal("3"))).isZero();
    Assertions.assertThat(plus.apply(null, null)).isNull();

    Assertions.assertThat(((BigDecimal) minus.apply(null, new BigDecimal("2"))).compareTo(new BigDecimal("2"))).isZero();
    Assertions.assertThat(((BigDecimal) minus.apply(new BigDecimal("3"), null)).compareTo(new BigDecimal("3"))).isZero();

    Assertions.assertThat(multiply.apply(null, new BigDecimal("2"))).isNull();
    Assertions.assertThat(multiply.apply(new BigDecimal("3"), null)).isNull();
    Assertions.assertThat(divide.apply(null, new BigDecimal("2"))).isNull();
    Assertions.assertThat(divide.apply(new BigDecimal("3"), null)).isNull();
  }

  @Test
  void testComparisonDivideAsBigDecimal() {
    BiFunction<Number, Number, Number> f = BinaryOperations.createComparisonBiFunction(ComparisonMethod.DIVIDE, BigDecimal.class);
    Number r = f.apply(new BigDecimal("7"), new BigDecimal("12"));
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("7").divide(new BigDecimal("12"), MathContext.DECIMAL128))).isZero();
  }

  @Test
  void testComparisonAbsoluteDifferenceAsBigDecimal() {
    BiFunction<Number, Number, Number> f = BinaryOperations.createComparisonBiFunction(ComparisonMethod.ABSOLUTE_DIFFERENCE, BigDecimal.class);
    Number r = f.apply(new BigDecimal("5"), new BigDecimal("3"));
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("2"))).isZero();
  }

  @Test
  void testComparisonRelativeDifferenceAsBigDecimal() {
    BiFunction<Number, Number, Number> f = BinaryOperations.createComparisonBiFunction(ComparisonMethod.RELATIVE_DIFFERENCE, BigDecimal.class);
    Number r = f.apply(new BigDecimal("6"), new BigDecimal("4"));
    Assertions.assertThat(r).isInstanceOf(BigDecimal.class);
    Assertions.assertThat(((BigDecimal) r).compareTo(new BigDecimal("0.5"))).isZero();
    Assertions.assertThat(f.apply(new BigDecimal("6"), null)).isNull();
    Assertions.assertThat(f.apply(null, new BigDecimal("4"))).isNull();
  }
}
