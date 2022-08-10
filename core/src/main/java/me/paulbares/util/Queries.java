package me.paulbares.util;

import me.paulbares.query.dto.ExplicitOrderDto;
import me.paulbares.query.dto.OrderDto;
import me.paulbares.query.dto.SimpleOrderDto;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static me.paulbares.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> orderToComparator(Map<String, OrderDto> orders) {
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
    return res;
  }
}
