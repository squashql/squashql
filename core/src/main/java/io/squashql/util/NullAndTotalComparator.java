package io.squashql.util;

import io.squashql.query.database.QueryEngine;

import java.io.Serializable;
import java.util.Comparator;

public class NullAndTotalComparator<T> implements Comparator<T>, Serializable {

  private final boolean nullFirst;
  private final boolean totalFirst;
  // if null, non-null Ts are considered equal
  @SuppressWarnings("serial") // Not statically typed as Serializable
  private final Comparator<T> real;

  NullAndTotalComparator(boolean nullFirst, boolean totalFirst, Comparator<? super T> real) {
    this.nullFirst = nullFirst;
    this.totalFirst = totalFirst;
    this.real = (Comparator<T>) real;
  }

  @Override
  public int compare(T a, T b) {
    if (a == null) {
      return (b == null) ? 0 : (this.nullFirst ? -1 : 1);
    } else if (b == null) {
      return this.nullFirst ? 1 : -1;
    } else {
      if (isTotal(a)) {
        if (isTotal(b)) {
          return 0;
        }
        return this.totalFirst ? -1 : 1;
      } else if (isTotal(b)) {
        if (isTotal(a)) {
          return 0;
        }
        return this.totalFirst ? 1 : -1;
      }
      return (this.real == null) ? 0 : this.real.compare(a, b);
    }
  }

  private static <T> boolean isTotal(T a) {
    return a.equals(QueryEngine.TOTAL) || a.equals(QueryEngine.GRAND_TOTAL);
  }

  public static <T> NullAndTotalComparator<T> nullsLastAndTotalsFirst(Comparator<? super T> comparator) {
    return new NullAndTotalComparator<>(false, true, comparator);
  }
}


