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
    return Collections.emptyMap();
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
  public Map<QueryExecutor.QueryScope, Set<Measure>> visit(ComparisonMeasureReferencePosition cmrp) {
    if (cmrp.ancestors != null && !MeasureUtils.isPrimitive(cmrp.measure)) {
      // Not support for the moment
      // Only for parent because it uses rollup during the prefetch so subtotals are computed by the database, not AITM.
      throw new IllegalArgumentException("Only a primitive measure can be used in a parent comparison measure");
    }
    QueryExecutor.QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.query, cmrp, this.originalQueryScope, this.fieldSupplier);
    Map<QueryExecutor.QueryScope, Set<Measure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(cmrp.measure)));
    result.put(readScope, Set.of(cmrp.measure));
    return result;
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
