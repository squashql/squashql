package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;

import java.util.*;
import java.util.function.Function;

public class MeasurePrefetcherVisitor implements MeasureVisitor<Map<QueryExecutor.QueryScope, Set<Measure>>> {

  private final QueryExecutor.QueryScope originalQueryScope;
  private final Function<String, Field> fieldSupplier;

  public MeasurePrefetcherVisitor(QueryExecutor.QueryScope originalQueryScope, Function<String, Field> fieldSupplier) {
    this.originalQueryScope = originalQueryScope;
    this.fieldSupplier = fieldSupplier;
  }

  private Map<QueryExecutor.QueryScope, Set<Measure>> original() {
    return Map.of(this.originalQueryScope, Collections.emptySet());
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(AggregatedMeasure measure) {
    return original();
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(ExpressionMeasure measure) {
    return original();
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(BinaryOperationMeasure measure) {
    return Map.of(this.originalQueryScope, MutableSetFactoryImpl.INSTANCE.of(measure.leftOperand, measure.rightOperand));
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(ComparisonMeasureReferencePosition measure) {
    return Map.of(this.originalQueryScope, Set.of(measure.measure));
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(ParentComparisonMeasure measure) {
    QueryExecutor.QueryScope parentScope = MeasureUtils.getParentScope(this.originalQueryScope, measure, this.fieldSupplier);
    if (!MeasureUtils.isPrimitive(measure.measure)) {
      // Not support for the moment
      throw new IllegalArgumentException("Only a primitive measure can be used in a parent comparison measure");
    }
    return Map.of(parentScope, Set.of(measure.measure), this.originalQueryScope, Set.of(measure.measure));
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(LongConstantMeasure measure) {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(DoubleConstantMeasure measure) {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(UnresolvedExpressionMeasure measure) {
    throw new IllegalStateException();
  }
}
