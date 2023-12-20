package io.squashql.query.compiled;

public interface MeasureVisitor<R> {

  R visit(CompiledAggregatedMeasure measure);

  R visit(CompiledExpressionMeasure measure);

  R visit(CompiledBinaryOperationMeasure measure);

  R visit(CompiledComparisonMeasure measure);

  R visit(CompiledConstantMeasure measure);

}
