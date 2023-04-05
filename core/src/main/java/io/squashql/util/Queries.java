package io.squashql.util;

import io.squashql.query.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.dto.*;
import io.squashql.store.Field;

import java.util.*;
import java.util.function.Function;

import static io.squashql.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> getComparators(QueryDto queryDto) {
    Map<String, OrderDto> orders = queryDto.orders;
    Map<String, Comparator<?>> res = new HashMap<>();
    orders.forEach((c, order) -> {
      if (order instanceof SimpleOrderDto so) {
        res.put(c, NullAndTotalComparator.nullsLastAndTotalsFirst(so.order == DESC ? Comparator.naturalOrder().reversed() : Comparator.naturalOrder()));
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(c, NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(eo.explicit)));
      } else {
        throw new IllegalStateException("Unexpected value: " + orders);
      }
    });

    // Special case for Bucket that defines implicitly an order.
    ColumnSet bucket = queryDto.columnSets.get(ColumnSetKey.BUCKET);
    if (bucket != null) {
      BucketColumnSetDto cs = (BucketColumnSetDto) bucket;
      Map<Object, List<Object>> m = new LinkedHashMap<>();
      cs.values.forEach((k, v) -> {
        List<Object> l = new ArrayList<>();
        l.addAll(v);
        m.put(k, l);
      });
      res.put(cs.name, new CustomExplicitOrdering(new ArrayList<>(m.keySet())));
      res.put(cs.field, DependentExplicitOrdering.create(m));
    }

    return res;
  }

  public static DatabaseQuery queryScopeToDatabaseQuery(QueryExecutor.QueryScope queryScope, Function<String, Field> fieldSupplier, int limit) {
    Set<Field> selects = new HashSet<>(queryScope.columns());
    DatabaseQuery prefetchQuery = new DatabaseQuery();
    if (queryScope.tableDto() != null) {
      prefetchQuery.table(queryScope.tableDto());
    } else if (queryScope.subQuery() != null) {
      prefetchQuery.subQuery(toSubDatabaseQuery(queryScope.subQuery(), fieldSupplier));
    } else {
      throw new IllegalArgumentException("A table or sub-query was expected in " + queryScope);
    }
    prefetchQuery.whereCriteriaDto = queryScope.whereCriteriaDto();
    prefetchQuery.havingCriteriaDto = queryScope.havingCriteriaDto();
    selects.forEach(prefetchQuery::withSelect);
    Optional.ofNullable(queryScope.rollupColumns()).ifPresent(r -> r.forEach(prefetchQuery::withRollup));
    prefetchQuery.limit(limit);
    prefetchQuery.cte(queryScope.cte());
    return prefetchQuery;
  }

  public static DatabaseQuery toSubDatabaseQuery(QueryDto query, Function<String, Field> fieldSupplier) {
    if (query.subQuery != null) {
      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
    }

    Set<String> cols = new HashSet<>(query.columns);
    if (query.columnSets != null && !query.columnSets.isEmpty()) {
      throw new IllegalArgumentException("column sets are not expected in sub query: " + query);
    }
    if (query.context != null && !query.context.isEmpty()) {
      throw new IllegalArgumentException("context values are not expected in sub query: " + query);
    }

    for (Measure measure : query.measures) {
      if (measure instanceof AggregatedMeasure
              || measure instanceof ExpressionMeasure
              || measure instanceof BinaryOperationMeasure) {
        continue;
      }
      throw new IllegalArgumentException("Only "
              + AggregatedMeasure.class.getSimpleName() + ", "
              + ExpressionMeasure.class.getSimpleName() + " or "
              + BinaryOperationMeasure.class.getSimpleName() + " can be used in a sub-query but "
              + measure + " was provided");
    }

    DatabaseQuery prefetchQuery = new DatabaseQuery().table(query.table);
    prefetchQuery.whereCriteriaDto = query.whereCriteriaDto;
    prefetchQuery.havingCriteriaDto = query.havingCriteriaDto;
    cols.stream().map(fieldSupplier).forEach(prefetchQuery::withSelect);
    query.measures.forEach(prefetchQuery::withMeasure);
    return prefetchQuery;
  }
}
