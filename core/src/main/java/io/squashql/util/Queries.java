package io.squashql.util;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.Field;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;

import java.util.*;

import static io.squashql.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> getSquashQLComparators(QueryDto queryDto) {
    Map<Field, OrderDto> orders = queryDto.orders;
    Map<String, Comparator<?>> res = new LinkedHashMap<>(); // order is important !
    orders.forEach((c, order) -> {
      if (order instanceof SimpleOrderDto so) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(so.order == DESC ? Comparator.naturalOrder().reversed() : Comparator.naturalOrder()));
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(eo.explicit)));
      } else {
        throw new IllegalStateException("Unexpected value: " + orders);
      }
    });

    // Special case for group that defines implicitly an order.
    ColumnSet group = queryDto.columnSets.get(ColumnSetKey.GROUP);
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
