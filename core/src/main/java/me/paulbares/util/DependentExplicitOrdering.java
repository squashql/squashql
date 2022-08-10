package me.paulbares.util;

import java.io.Serializable;
import java.util.*;

/**
 * Similar to {@code com.google.common.collect.ExplicitOrdering} but do not throw a
 * {@code com.google.common.collect.Ordering.IncomparableValueException} when value is not in the given list.
 */
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DependentExplicitOrdering that = (DependentExplicitOrdering) o;
    return Objects.equals(this.comp, that.comp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.comp);
  }
}
