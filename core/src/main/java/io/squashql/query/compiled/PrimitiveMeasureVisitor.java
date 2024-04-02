package io.squashql.query.compiled;

public class PrimitiveMeasureVisitor implements CompiledMeasureVisitor<Boolean> {

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
  public Boolean visit(CompiledDoubleConstantMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(CompiledLongConstantMeasure measure) {
    return true;
  }

  @Override
  public Boolean visit(CompiledComparisonMeasureReferencePosition measure) {
    return false;
  }

  @Override
  public Boolean visit(CompiledGrandTotalComparisonMeasure compiledGrandTotalComparisonMeasure) {
    return false;
  }

  @Override
  public Boolean visit(CompiledVectorAggMeasure measure) {
    // This measure is "indirectly" computed by the underlying DB, but it is not a primitive. See PrefetchVisitor.
    return false;
  }

  @Override
  public Boolean visit(CompiledVectorTupleAggMeasure compiledVectorTupleAggMeasure) {
    // This measure is "indirectly" computed by the underlying DB, but it is not a primitive. See PrefetchVisitor.
    return false;
  }
}
