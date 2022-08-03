package me.paulbares.util;

import com.google.common.collect.Ordering;
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
 *
 * @param <T> the type of objects that may be compared by this comparator
 */
public class CustomExplicitOrdering<T extends Comparable<? super T>> extends Ordering<T> implements Serializable {
  final ImmutableObjectIntMap<T> rankMap;

  public CustomExplicitOrdering(List<T> valuesInOrder) {
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
  public int compare(T left, T right) {
    final Integer rankLeft = this.rankMap.getIfAbsent(left, -1);
    final Integer rankRight = this.rankMap.getIfAbsent(right, -1);

    if (rankLeft >= 0) {
      if (rankRight >= 0) {
        return rankLeft - rankRight;
      } else {
        return -1; // left is in the list, not right. So left is before
      }
    } else if (rankRight >= 0) {
      return 1;
    } else {
      Comparator<T> comparator = Comparator.naturalOrder();
      return comparator.compare(left, right);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CustomExplicitOrdering) {
      CustomExplicitOrdering<?> that = (CustomExplicitOrdering<?>) object;
      return this.rankMap.equals(that.rankMap);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.rankMap.hashCode();
  }

  @Override
  public String toString() {
    return "Ordering.explicit(" + this.rankMap.keySet() + ")";
  }
}
