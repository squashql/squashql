package io.squashql.query.compiled;

import io.squashql.query.ComparisonMethod;

import java.util.function.BiFunction;

/**
 * Interface for comparison measure.
 */
public interface CompiledComparisonMeasure extends CompiledMeasure {

  CompiledMeasure measure();

  ComparisonMethod comparisonMethod();

  BiFunction<Object, Object, Object> comparisonOperator();
}
