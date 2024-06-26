package io.squashql.query.compiled;

public interface CompiledMeasureVisitor<R> {

  R visit(CompiledAggregatedMeasure measure);

  R visit(CompiledExpressionMeasure measure);

  R visit(CompiledBinaryOperationMeasure measure);

  R visit(CompiledComparisonMeasureReferencePosition measure);

  R visit(CompiledGrandTotalComparisonMeasure measure);

  R visit(CompiledDoubleConstantMeasure measure);

  R visit(CompiledLongConstantMeasure measure);

  R visit(CompiledVectorAggMeasure measure);

  R visit(CompiledVectorTupleAggMeasure measure);
}
