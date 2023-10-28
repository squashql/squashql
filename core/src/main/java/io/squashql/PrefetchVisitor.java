package io.squashql;

import io.squashql.query.*;
import io.squashql.query.QueryExecutor.QueryScope;
import io.squashql.query.dto.QueryDto;
import lombok.RequiredArgsConstructor;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class PrefetchVisitor implements MeasureVisitor<Map<QueryScope, Set<Measure>>> {

  private final QueryDto query;
  private final QueryScope originalQueryScope;

  private Map<QueryScope, Set<Measure>> empty() {
    return Collections.emptyMap();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(AggregatedMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(ExpressionMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(BinaryOperationMeasure measure) {
    if (new PrimitiveMeasureVisitor().visit(measure)) {
      return empty();
    } else {
      return Map.of(this.originalQueryScope, MutableSetFactoryImpl.INSTANCE.of(measure.leftOperand, measure.rightOperand));
    }
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(ComparisonMeasureReferencePosition cmrp) {
    QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.query, cmrp, this.originalQueryScope);
    Map<QueryScope, Set<Measure>> result = new HashMap<>(Map.of(this.originalQueryScope, Set.of(cmrp.measure)));
    result.put(readScope, Set.of(cmrp.measure));
    return result;
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(LongConstantMeasure measure) {
    return empty();
  }

  @Override
  public Map<QueryScope, Set<Measure>> visit(DoubleConstantMeasure measure) {
    return empty();
  }
}
