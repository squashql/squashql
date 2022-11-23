package me.paulbares.query;

public interface MeasureVisitor<R> {

  R visit(AggregatedMeasure measure);

  R visit(ExpressionMeasure measure);

  R visit(BinaryOperationMeasure measure);

  R visit(ComparisonMeasureReferencePosition measure);

  R visit(LongConstantMeasure measure);

  R visit(DoubleConstantMeasure measure);
}
