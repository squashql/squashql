package io.squashql;

import io.squashql.query.*;

public class PrimitiveMeasureVisitor implements MeasureVisitor<Boolean> {
  @Override
  public Boolean visit(AggregatedMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(ExpressionMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(BinaryOperationMeasure measure) {
    return measure.leftOperand.accept(this) && measure.rightOperand.accept(this);
  }

  @Override
  public Boolean visit(LongConstantMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(DoubleConstantMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(ComparisonMeasureReferencePosition measure) {
    return false;
  }
}
