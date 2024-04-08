package io.squashql.query.measure.visitor;

import io.squashql.query.*;
import io.squashql.query.measure.ParametrizedMeasure;

public interface MeasureVisitor<R> {

  R visit(AggregatedMeasure measure);

  R visit(ExpressionMeasure measure);

  R visit(BinaryOperationMeasure measure);

  R visit(ComparisonMeasureReferencePosition measure);

  R visit(ComparisonMeasureGrandTotal measure);

  R visit(DoubleConstantMeasure measure);

  R visit(LongConstantMeasure measure);

  R visit(VectorAggMeasure measure);

  R visit(VectorTupleAggMeasure measure);

  R visit(ParametrizedMeasure measure);

  R visit(PartialHierarchicalComparisonMeasure measure);
}
