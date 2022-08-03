package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.*;

public class TableOrderer2 {

  public enum Order {
    ASC, DESC
  }

  public class OrderByDto {
    String column;
    Order order;
  }

  // FIXME first values comparator.
  public static void order(ColumnarTable table, List<OrderByDto> orders) {
    for (OrderByDto order : orders) {
      String c = order.column;
      Order o = order.order;
      List<Object> columnValues = table.getColumnValues(c); // to reorder
//      Collections.so
    }
  }

  public static class IndirectSorter<T extends Comparable<T>> {
    public int[] sort(T args[]) {
      Integer originalIndex[] = new Integer[args.length];
      int newIndex[] = new int[args.length];
      for (int i = 0; i < originalIndex.length; i++) {
        originalIndex[i] = i;
      }
      Arrays.sort(originalIndex, new IndirectCompareClass<>(args));
      for (int i = 0; i < originalIndex.length; i++) {
        newIndex[i] = originalIndex[i].intValue();
      }
      return newIndex;
    }

    class IndirectCompareClass<T extends Comparable<T>> implements Comparator<Integer> {
      T args[];

      public IndirectCompareClass(T args[]) {
        this.args = args;
      }

      public int compare(Integer in1, Integer in2) {
        return this.args[in1.intValue()].compareTo(this.args[in2.intValue()]);
      }

      public boolean equals(Integer in1, Integer in2) {
        return this.args[in1.intValue()].equals(this.args[in2.intValue()]);
      }
    }
  }

  public static void main(String[] args) {
    List<Comparable<?>> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c");
    List<Comparable<?>> c2 = List.of( 1,   2,   3,   1,   2,   4,   3,   1);
//    List<Object> c3 = List.of( 1,   2,   3,   1,   2,   4,   3,   1);
 // FIXME

    Integer[] finalMapping = new Integer[c1.size()];

    List<Comparable<?>> prev = c1;
    List<Comparable<?>> next = c2;

    TreeMap<Object, List<Integer>> bucket = new TreeMap<>();
    TreeMap<Object, List<Comparable<?>>> bucketValues = new TreeMap<>();
    for (int i = 0; i < prev.size(); i++) {
      bucket.computeIfAbsent(prev.get(i), k -> new ArrayList<>()).add(i);
      bucketValues.computeIfAbsent(prev.get(i), k -> new ArrayList<>()).add(next.get(i));
    }

    int offset = 0;
    for (List<Integer> value : bucket.values()) {
      System.arraycopy(value.toArray(new Integer[0]), 0, finalMapping, offset, value.size());
      offset += value.size();
    }

    prev = bucketValues.values().iterator().next();

//    for (Map.Entry<Object, List<Comparable<?>>> entry : bucketValues.entrySet()) {
//      IndirectSorter sorter = new IndirectSorter<>();
//      int[] localIndices = sorter.sort(entry.getValue().toArray(new Comparable[0]));
//      List<Integer> globalIndices = bucket.get(entry.getKey());
//      int[] indices = new int[globalIndices.size()];
//      for (int i = 0; i < globalIndices.size(); i++) {
//        indices[i] = globalIndices.get(localIndices[i]);
//      }
//      System.out.println();
//    }

    List<Field> headers = Arrays.asList(new Field("c1", String.class), new Field("c2", String.class));
//    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0], List.of(c1, c2))
//            .show();
//
//    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0],
//            List.of(reorder(c1, finalOrder), reorder(c2, finalOrder)))
//            .show();
  }

  private static List<Object> reorder(List<Object> list, List<Integer> order) {
    List<Object> c1Copy = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      c1Copy.set(i, list.get(order.get(i)));
    }
    return c1Copy;
  }
}
