package me.paulbares;

import me.paulbares.query.*;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class MeasurePrefetcherVisitor implements MeasureVisitor<Map<QueryExecutor.QueryScope, Set<Measure>>> {

  private final QueryDto query;
  private final QueryExecutor.QueryScope originalQueryScope;
  private final Function<String, Field> fieldSupplier;

  public MeasurePrefetcherVisitor(QueryDto query, QueryExecutor.QueryScope originalQueryScope, Function<String, Field> fieldSupplier) {
    this.query = query;
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
    QueryExecutor.QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.query, this.originalQueryScope);
    Map<QueryExecutor.QueryScope, Set<Measure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(measure.measure)));
    result.put(readScope, Set.of(measure.measure));
    return result;
  }

  @Override
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(ParentComparisonMeasure measure) {
    if (!MeasureUtils.isPrimitive(measure.measure)) {
      // Not support for the moment
      throw new IllegalArgumentException("Only a primitive measure can be used in a parent comparison measure");
    }
    QueryExecutor.QueryScope parentScope = MeasureUtils.getParentScopeWithClearedConditions(this.originalQueryScope, measure, this.fieldSupplier);
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