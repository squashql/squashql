package io.squashql;

import io.squashql.query.*;
import io.squashql.query.QueryExecutor.QueryScope;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.FieldWithStore;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class PrefetchVisitor implements MeasureVisitor<Map<QueryScope, Set<Measure>>> {

  private final QueryDto query;
  private final QueryScope originalQueryScope;
  private final Function<String, FieldWithStore> fieldSupplier;

  public PrefetchVisitor(QueryDto query, QueryScope originalQueryScope, Function<String, FieldWithStore> fieldSupplier) {
    this.query = query;
    this.originalQueryScope = originalQueryScope;
    this.fieldSupplier = fieldSupplier;
  }

  private Map<QueryScope, Set<Measure>> original() {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(AggregatedMeasure measure) {
    return original();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(ExpressionMeasure measure) {
    return original();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(BinaryOperationMeasure measure) {
    return Map.of(this.originalQueryScope, MutableSetFactoryImpl.INSTANCE.of(measure.leftOperand, measure.rightOperand));
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(ComparisonMeasureReferencePosition cmrp) {
    QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.query, cmrp, this.originalQueryScope, this.fieldSupplier);
    Map<QueryScope, Set<Measure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(cmrp.measure)));
    result.put(readScope, Set.of(cmrp.measure));
    return result;
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(LongConstantMeasure measure) {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(DoubleConstantMeasure measure) {
    return Collections.emptyMap();
  }
}
