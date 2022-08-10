package me.paulbares.util;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.dto.*;

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
        Comparator<?> comp = Comparator.naturalOrder();
        res.put(c, so.order == DESC ? comp.reversed() : comp);
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(c, new CustomExplicitOrdering(eo.explicit));
      } else {
        throw new IllegalStateException("Unexpected value: " + orders);
      }
    });

    // Special case for Bucket that defines implicitly an order.
    ColumnSet bucket = queryDto.columnSets.get(QueryDto.BUCKET);
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
}
