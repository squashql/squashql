package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.*;

public class TableOrdererComparatorOnly {

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
    public int[] sort(List<List<T>> args, List<Comparator<T>> comparators) {
      int length = args.get(0).size();
      Integer originalIndex[] = new Integer[length];
      int newIndex[] = new int[length];
      for (int i = 0; i < originalIndex.length; i++) {
        originalIndex[i] = i;
      }
      Arrays.sort(originalIndex, new IndirectCompareClass<>(args, comparators));
      for (int i = 0; i < originalIndex.length; i++) {
        newIndex[i] = originalIndex[i].intValue();
      }
      return newIndex;
    }

    class IndirectCompareClass<T extends Comparable<T>> implements Comparator<Integer> {
      List<List<T>> argsList;
      List<Comparator<T>> comparators;

      public IndirectCompareClass(List<List<T>> argsList, List<Comparator<T>> comparators) {
        this.argsList = argsList;
        this.comparators = comparators;
      }

      public int compare(Integer in1, Integer in2) {
        for (int i = 0; i < this.argsList.size(); i++) {
          Comparator<T> comp = this.comparators.get(i);
          int compare = comp.compare(this.argsList.get(i).get(in1.intValue()), this.argsList.get(i).get(in2.intValue()));
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }

      public boolean equals(Integer in1, Integer in2) {
        for (int i = 0; i < this.argsList.size(); i++) {
          Comparator<T> comp = this.comparators.get(i);
          int compare = comp.compare(this.argsList.get(i).get(in1.intValue()), this.argsList.get(i).get(in2.intValue()));
          if (compare != 0) {
            return false;
          }
        }
        return true;
      }
    }
  }

  public static void main(String[] args) {
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c", "a");
    List<Object> c2 = List.of(1, 2, 3, 1, 2, 4, 3, 1, 1);
    List<Object> c3 = List.of(1, 2, 3, 2, 2, 4, 3, 5, 0);

    IndirectSorter tIndirectSorter = new IndirectSorter<>();
    int[] sort = tIndirectSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(Comparator.naturalOrder(), Comparator.naturalOrder(), Comparator.naturalOrder()));
    System.out.println(Arrays.toString(sort));

    List<Field> headers = Arrays.asList(
            new Field("c1", String.class),
            new Field("c2", String.class),
            new Field("c3", String.class));
    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0], List.of(c1, c2, c3))
            .show();

    new ColumnarTable(headers, Collections.emptyList(), new int[0], new int[0],
            List.of(reorder(c1, sort), reorder(c2, sort), reorder(c3, sort)))
            .show();
  }

  private static List<Object> reorder(List<?> list, int[] order) {
    List<Object> c1Copy = new ArrayList<>(list);
    for (int i = 0; i < list.size(); i++) {
      c1Copy.set(i, list.get(order[i]));
    }
    return c1Copy;
  }
}
