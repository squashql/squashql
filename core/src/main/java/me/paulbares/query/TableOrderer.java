package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.*;

public class TableOrderer {

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
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c");
    List<Object> c2 = List.of( 1,   2,   3,   1,   2,   4,   3,   1);
//    List<Object> c3 = List.of( 1,   2,   3,   1,   2,   4,   3,   1);
 // FIXME

    IndirectSorter<String> tIndirectSorter = new IndirectSorter<>();
    int[] sort = tIndirectSorter.sort(c1.toArray(new String[0]));
    System.out.println(Arrays.toString(sort));

    List<Integer> finalOrder = new ArrayList<>();

    List<Object> values = c1;
    TreeMap<Object, List<Integer>> m1 = new TreeMap<>();
    for (int i = 0; i < values.size(); i++) {
      m1
              .computeIfAbsent(values.get(i), k -> new ArrayList<>())
              .add(i);
    }
    System.out.println(m1);

    for (Map.Entry<Object, List<Integer>> entry : m1.entrySet()) {
      List<Object> v = new ArrayList<>();
      for (Integer integer : entry.getValue()) {
        v.add(c2.get(integer));
      }
      TreeMap<Object, List<Integer>> m2 = new TreeMap<>();
      for (int i = 0; i < v.size(); i++) {
        m2
                .computeIfAbsent(v.get(i), k -> new ArrayList<>())
                .add(entry.getValue().get(i)); // add real index
      }

      for (List<Integer> value : m2.values()) {
        finalOrder.addAll(value);
      }
      System.out.println(m2);
    }
    System.out.println(finalOrder);
    List<Field> headers = Arrays.asList(new Field("c1", String.class), new Field("c2", String.class));
    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0], List.of(c1, c2))
            .show();

    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0],
            List.of(reorder(c1, finalOrder), reorder(c2, finalOrder)))
            .show();
  }

  private static List<Object> reorder(List<Object> list, List<Integer> order) {
    List<Object> c1Copy = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      c1Copy.set(i, list.get(order.get(i)));
    }
    return c1Copy;
  }
}
