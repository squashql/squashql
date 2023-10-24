package io.squashql.util;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.Field;
import io.squashql.query.database.QueryResultFormat;
import io.squashql.query.dto.*;
import io.squashql.type.TypedField;

import java.util.*;
import java.util.stream.Collectors;

import static io.squashql.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> getComparators(QueryDto queryDto) {
    Map<Field, OrderDto> orders = queryDto.orders;
    Map<String, Comparator<?>> res = new HashMap<>();
    orders.forEach((c, order) -> {
      if (order instanceof SimpleOrderDto so) {
        res.put(c.name(), NullAndTotalComparator.nullsLastAndTotalsFirst(so.order == DESC ? Comparator.naturalOrder().reversed() : Comparator.naturalOrder()));
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(c.name(), NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(eo.explicit)));
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
      res.put(cs.name.name(), new CustomExplicitOrdering(new ArrayList<>(m.keySet())));
      res.put(cs.field.name(), DependentExplicitOrdering.create(m));
    }

    return res;
  }

  public static List<TypedField> generateGroupingSelect(QueryResultFormat format) {
    List<TypedField> selects = new ArrayList<>();
    selects.addAll(format.rollup());
    // order matters, this is why a LinkedHashSet is used.
    selects.addAll(format.groupingSets()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new)));
    return selects;
  }
}
