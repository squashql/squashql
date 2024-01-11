package io.squashql.util;

import java.util.ArrayList;
import java.util.List;

public final class ListUtils {

  private ListUtils() {
  }

  public static List<Object> reorder(List<?> list, int[] order) {
    List<Object> ordered = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      ordered.set(i, list.get(order[i]));
    }
    return ordered;
  }

  public static <T> List<T> reorder_(List<T> list, int[] order) {
    List<T> ordered = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      ordered.set(i, list.get(order[i]));
    }
    return ordered;
  }

  public static <T> List<T> createListWithNulls(int size) {
    List<T> l = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      l.add(null);
    }
    return l;
  }

  /**
   * R1 Estimate type. See https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample.
   */
  public static int percentileIndex(int length, double quantile) {
    if (length == 0) {
      return -1;
    } else if (length == 1) {
      return 0;
    } else {
      return (int) Math.floor(length * quantile);
    }
  }
}
