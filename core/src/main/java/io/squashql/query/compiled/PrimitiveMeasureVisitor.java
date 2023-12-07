package io.squashql.query.compiled;
//todo-mde should we remove in favor of method isPrimitive ?
public class PrimitiveMeasureVisitor implements MeasureVisitor<Boolean> {

  @Override
  public Boolean visit(CompiledAggregatedMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(CompiledExpressionMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(CompiledBinaryOperationMeasure measure) {
    return measure.leftOperand().accept(this) && measure.rightOperand().accept(this);
  }

  @Override
  public Boolean visit(CompiledConstantMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(CompiledComparisonMeasure measure) {
    return false;
  }
}
