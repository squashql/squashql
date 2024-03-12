package io.squashql.util;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.Field;
import io.squashql.query.QueryResolver;
import io.squashql.query.compiled.CompiledOrderBy;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.type.TypedField;

import java.util.*;
import java.util.stream.Collectors;

import static io.squashql.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  /**
   * Creates {@link Comparator} for each order by from the query {@link QueryResolver#getQuery()}. If all is set to true,
   * it also creates comparator for orderBy that can be computed on the DB side, otherwise only the comparator for the
   * fields "computed" by squashql i.e. squashql measures or {@link GroupColumnSetDto}.
   */
  public static Map<String, Comparator<?>> getSquashQLComparators(QueryResolver queryResolver, boolean all) {
    Map<Field, OrderDto> orders = queryResolver.getQuery().orders;
    Map<String, Comparator<?>> res = new LinkedHashMap<>(); // order is important !
    Set<TypedField> fieldOrderedInDB = queryResolver.getCompiledOrderByInDB().stream().map(CompiledOrderBy::field).collect(Collectors.toSet());
    orders.forEach((c, order) -> {
      if (!all && fieldOrderedInDB.contains(queryResolver.resolveField(c))) {
        return;
      }

      if (order instanceof SimpleOrderDto so) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(so.order == DESC ? Comparator.naturalOrder().reversed() : Comparator.naturalOrder()));
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(eo.explicit)));
      } else {
        throw new IllegalStateException("Unexpected value: " + orders);
      }
    });

    // Special case for group that defines implicitly an order.
    ColumnSet group = queryResolver.getQuery().columnSets.get(ColumnSetKey.GROUP);
    if (group != null) {
      GroupColumnSetDto cs = (GroupColumnSetDto) group;
      Map<Object, List<Object>> m = new LinkedHashMap<>();
      cs.values.forEach((k, v) -> {
        List<Object> l = new ArrayList<>(v);
        m.put(k, l);
      });
      res.put(SqlUtils.squashqlExpression(cs.newField), new CustomExplicitOrdering(new ArrayList<>(m.keySet())));
      res.put(SqlUtils.squashqlExpression(cs.field), DependentExplicitOrdering.create(m));
    }

    return res;
  }
}
