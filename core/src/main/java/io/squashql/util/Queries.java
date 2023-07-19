package io.squashql.util;

import io.squashql.PrimitiveMeasureVisitor;
import io.squashql.query.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.dto.*;
import io.squashql.store.TypedField;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        List<Object> l = new ArrayList<>(v);
        m.put(k, l);
      });
      res.put(cs.name, new CustomExplicitOrdering(new ArrayList<>(m.keySet())));
      res.put(cs.field, DependentExplicitOrdering.create(m));
    }

    return res;
  }

  public static DatabaseQuery queryScopeToDatabaseQuery(QueryExecutor.QueryScope queryScope, Function<String, TypedField> fieldSupplier, int limit) {
    Set<TypedField> selects = new HashSet<>(queryScope.columns());
    DatabaseQuery prefetchQuery = new DatabaseQuery();
    if (queryScope.tableDto() != null) {
      prefetchQuery.table(queryScope.tableDto());
    } else if (queryScope.subQuery() != null) {
      prefetchQuery.subQuery(toSubDatabaseQuery(queryScope.subQuery(), fieldSupplier));
    } else {
      throw new IllegalArgumentException("A table or sub-query was expected in " + queryScope);
    }
    prefetchQuery.whereCriteria(queryScope.whereCriteriaDto());
    prefetchQuery.havingCriteria(queryScope.havingCriteriaDto());
    selects.forEach(prefetchQuery::withSelect);
    prefetchQuery.rollup(queryScope.rollupColumns());
    prefetchQuery.groupingSets(queryScope.groupingSets());
    prefetchQuery.limit(limit);
    prefetchQuery.virtualTable(queryScope.virtualTableDto());
    return prefetchQuery;
  }

  public static DatabaseQuery toSubDatabaseQuery(QueryDto query, Function<String, TypedField> fieldSupplier) {
    if (query.subQuery != null) {
      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
    }

    if (query.virtualTableDto != null) {
      throw new IllegalArgumentException("virtualTableDto in a sub-query is not supported");
    }

    Set<String> cols = new HashSet<>(query.columns);
    if (query.columnSets != null && !query.columnSets.isEmpty()) {
      throw new IllegalArgumentException("column sets are not expected in sub query: " + query);
    }
    if (query.parameters != null && !query.parameters.isEmpty()) {
      throw new IllegalArgumentException("parameters are not expected in sub query: " + query);
    }

    for (Measure measure : query.measures) {
      if (measure.accept(new PrimitiveMeasureVisitor())) {
        continue;
      }
      throw new IllegalArgumentException("Only measures that can be computed by the underlying database can be used" +
              " in a sub-query but " + measure + " was provided");
    }

    DatabaseQuery prefetchQuery = new DatabaseQuery().table(query.table);
    prefetchQuery.whereCriteriaDto = query.whereCriteriaDto;
    prefetchQuery.havingCriteriaDto = query.havingCriteriaDto;
    cols.stream().map(fieldSupplier).forEach(prefetchQuery::withSelect);
    query.measures.forEach(prefetchQuery::withMeasure);
    return prefetchQuery;
  }

  public static List<TypedField> generateGroupingSelect(DatabaseQuery query) {
    List<TypedField> selects = new ArrayList<>();
    selects.addAll(query.rollup);
    // order matters, this is why a LinkedHashSet is used.
    selects.addAll(query.groupingSets
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new)));
    return selects;
  }
}
