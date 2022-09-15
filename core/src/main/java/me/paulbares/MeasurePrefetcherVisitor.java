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
    List<QueryExecutor.QueryScope> parentScopes = MeasureUtils.getParentScopes(this.originalQueryScope, measure, this.fieldSupplier);
    Map<QueryExecutor.QueryScope, Set<Measure>> r = new HashMap<>();
    parentScopes.forEach(s -> r.put(s, Set.of(measure.measure)));
    return r;
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
