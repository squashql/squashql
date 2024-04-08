package io.squashql.query.measure.visitor;

import io.squashql.query.*;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import io.squashql.table.PivotTableContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            measure.elements,
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
    if (measure.key.equals(Repository.INCREMENTAL_VAR)) {
      Object axis = measure.parameters.get("axis");
      if (axis != null || !measure.parameters.containsKey("ancestors")) {
        List<Field> ancestors = getAncestors((Axis) axis);
        Map<String, Object> copy = new HashMap<>(measure.parameters);
        copy.remove("axis");
        copy.put("ancestors", ancestors);
        return new ParametrizedMeasure(measure.alias, Repository.INCREMENTAL_VAR, copy);
      }
    }
    return measure;
  }

  @Override
  public Measure visit(PartialComparisonAncestorsMeasure measure) {
    List<Field> ancestors = getAncestors(measure.axis);
    return new ComparisonMeasureReferencePosition(
            measure.alias,
            measure.comparisonMethod,
            measure.measure.accept(this),
            ancestors,
            measure.grandTotalAlongAncestors
    );
  }

  private List<Field> getAncestors(Axis axis) {
    List<Field> ancestors = axis != null ? switch (axis) {
      case ROW -> this.pivotTableContext.cleansedColumns;
      case COLUMN -> this.pivotTableContext.cleansedRows;
    } : this.pivotTableContext.cleansedColumns;
    return ancestors;
  }
}
