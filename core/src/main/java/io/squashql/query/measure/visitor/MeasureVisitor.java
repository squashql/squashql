package io.squashql.query.measure.visitor;

import io.squashql.query.ComparisonMeasureGrandTotal;
import io.squashql.query.ComparisonMeasureReferencePosition;
import io.squashql.query.LongConstantMeasure;
import io.squashql.query.PartialComparisonAncestorsMeasure;
import io.squashql.query.measure.*;

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

  R visit(PartialComparisonAncestorsMeasure measure);
}
