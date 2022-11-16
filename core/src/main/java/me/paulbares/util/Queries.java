package me.paulbares.util;

import me.paulbares.query.*;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.dto.*;
import me.paulbares.store.Field;

import java.util.*;

import static me.paulbares.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> getComparators(QueryDto queryDto) {
    Map<String, OrderDto> orders = queryDto.orders;
    Map<String, Comparator<?>> res = new HashMap<>();
    orders.forEach((c, order) -> {
      if (order instanceof SimpleOrderDto so) {
        Comparator<?> comp = Comparator.nullsLast(Comparator.naturalOrder());
        res.put(c, so.order == DESC ? comp.reversed() : comp);
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(c, new CustomExplicitOrdering(eo.explicit));
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

  public static DatabaseQuery queryScopeToDatabaseQuery(QueryExecutor.QueryScope queryScope) {
    Set<String> selects = new HashSet<>();
    queryScope.columns().stream().map(Field::name).forEach(selects::add);
    DatabaseQuery prefetchQuery = new DatabaseQuery();
    if (queryScope.tableDto() != null) {
      prefetchQuery.table(queryScope.tableDto());
    } else if (queryScope.subQuery() != null) {
      prefetchQuery.subQuery(toSubDatabaseQuery(queryScope.subQuery()));
    } else {
      throw new IllegalArgumentException("A table or sub-query was expected in " + queryScope);
    }
    prefetchQuery.conditions = queryScope.conditions();
    selects.forEach(prefetchQuery::withSelect);
    Optional.ofNullable(queryScope.rollUpColumns()).ifPresent(r -> r.stream().map(Field::name).forEach(prefetchQuery::withRollUp));
    return prefetchQuery;
  }

  public static DatabaseQuery toSubDatabaseQuery(QueryDto query) {
    if (query.subQuery != null) {
      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
    }

    Set<String> cols = new HashSet<>();
    query.columns.forEach(cols::add);
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
    prefetchQuery.conditions = query.conditions;
    cols.forEach(prefetchQuery::withSelect);
    query.measures.forEach(prefetchQuery::withMeasure);
    return prefetchQuery;
  }
}
