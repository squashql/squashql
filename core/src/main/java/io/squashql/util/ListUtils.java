package io.squashql.util;

import java.util.ArrayList;
import java.util.List;

public final class ListUtils {

  private ListUtils() {
  }

  public static <T> List<T> reorder(List<T> list, int[] order) {
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

//  public static <T> int index(List<T> list, T o) {
//    list.inde
//  }
}
