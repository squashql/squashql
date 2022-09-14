package me.paulbares.util;

import lombok.EqualsAndHashCode;
import org.eclipse.collections.api.map.primitive.ImmutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.MutableObjectIntMapFactoryImpl;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Similar to {@code com.google.common.collect.ExplicitOrdering} but do not throw a
 * {@code com.google.common.collect.Ordering.IncomparableValueException} when value is not in the given list.
 */
@EqualsAndHashCode
public class CustomExplicitOrdering implements Comparator<Object>, Serializable {
  final ImmutableObjectIntMap<?> rankMap;

  public CustomExplicitOrdering(List<?> valuesInOrder) {
    this.rankMap = indexMap(valuesInOrder);
  }

  static <E> ImmutableObjectIntMap<E> indexMap(Collection<E> list) {
    MutableObjectIntMap<E> builder = MutableObjectIntMapFactoryImpl.INSTANCE.empty();
    int i = 0;
    for (E e : list) {
      builder.put(e, i++);
    }
    return builder.toImmutable();
  }

  @Override
  public int compare(Object left, Object right) {
    final int rankLeft = this.rankMap.getIfAbsent(left, -1);
    final int rankRight = this.rankMap.getIfAbsent(right, -1);

    if (rankLeft >= 0) {
      if (rankRight >= 0) {
        return rankLeft - rankRight;
      } else {
        return -1; // left is in the list, not right. So left is before
      }
    } else if (rankRight >= 0) {
      return 1;
    } else {
      Comparator comparator = Comparator.naturalOrder();
      return comparator.compare(left, right);
    }
  }
}
