package io.squashql.query.measure.visitor;

import io.squashql.query.*;
import io.squashql.query.field.Field;
import io.squashql.query.measure.*;
import io.squashql.table.PivotTableContext;

import java.util.List;

public record PartialMeasureVisitor(
        PivotTableContext pivotTableContext) implements MeasureVisitor<Measure> {

  @Override
  public Measure visit(AggregatedMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(ExpressionMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(BinaryOperationMeasure measure) {
    return new BinaryOperationMeasure(measure.alias,
            measure.operator,
            measure.leftOperand.accept(this),
            measure.rightOperand.accept(this));
  }

  @Override
  public Measure visit(ComparisonMeasureReferencePosition measure) {
    return new ComparisonMeasureReferencePosition(
            measure.alias,
            measure.expression,
            measure.comparisonMethod,
            measure.comparisonOperator,
            measure.measure.accept(this),
            measure.columnSetKey,
            measure.referencePosition,
            measure.period,
            measure.ancestors,
            measure.grandTotalAlongAncestors);
  }

  @Override
  public Measure visit(ComparisonMeasureGrandTotal measure) {
    return new ComparisonMeasureGrandTotal(
            measure.alias,
            measure.expression,
            measure.comparisonMethod,
            measure.measure.accept(this));
  }

  @Override
  public Measure visit(DoubleConstantMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(LongConstantMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(VectorAggMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(VectorTupleAggMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(ParametrizedMeasure measure) {
    return measure;
  }

  @Override
  public Measure visit(PartialComparisonAncestorsMeasure measure) {
    List<Field> ancestors = measure.axis != null ? switch (measure.axis) {
      case ROW -> this.pivotTableContext.cleansedRows;
      case COLUMN -> this.pivotTableContext.cleansedColumns;
    } : this.pivotTableContext.cleansedColumns;
    return new ComparisonMeasureReferencePosition(
            measure.alias,
            measure.comparisonMethod,
            measure.measure.accept(this),
            ancestors,
            measure.grandTotalAlongAncestors
    );
  }
}
