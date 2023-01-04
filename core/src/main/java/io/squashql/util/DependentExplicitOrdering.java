package io.squashql.util;

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.*;

/**
 * Similar to {@code com.google.common.collect.ExplicitOrdering} but do not throw a
 * {@code com.google.common.collect.Ordering.IncomparableValueException} when value is not in the given list.
 */
@EqualsAndHashCode
public class DependentExplicitOrdering implements Comparator<Object>, Serializable {
  final Map<Object, Comparator<Object>> comp;

  private transient Object context;

  public DependentExplicitOrdering(Map<Object, Comparator<Object>> comp) {
    this.comp = comp;
  }

  @Override
  public int compare(Object left, Object right) {
    Objects.requireNonNull(this.context);
    Comparator<Object> c = this.comp.get(this.context);
    if (c == null) {
      return -1;
    }
    return c.compare(left, right);
  }

  public void setContext(Object context) {
    this.context = context;
  }

  public static DependentExplicitOrdering create(Map<Object, List<Object>> elements) {
    Map<Object, Comparator<Object>> res = new HashMap<>();
    elements.forEach((k, v) -> res.put(k, new CustomExplicitOrdering(v)));
    return new DependentExplicitOrdering(res);
  }
}
