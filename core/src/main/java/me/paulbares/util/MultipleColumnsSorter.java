package me.paulbares.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MultipleColumnsSorter {

  public static int[] sort(List<List<?>> args, List<Comparator<?>> comparators) {
    Integer originalIndex[] = new Integer[args.get(0).size()];
    for (int i = 0; i < originalIndex.length; i++) {
      originalIndex[i] = i;
    }
    Arrays.sort(originalIndex, new IndexComparator(args, comparators));
    return Arrays.stream(originalIndex).mapToInt(Integer::intValue).toArray();
  }

  private static class IndexComparator implements Comparator<Integer> {
    List<List<?>> argsList;
    List<Comparator<?>> comparators;

    public IndexComparator(List<List<?>> argsList, List<Comparator<?>> comparators) {
      this.argsList = argsList;
      this.comparators = comparators;
    }

    public int compare(Integer x, Integer y) {
      for (int i = 0; i < this.argsList.size(); i++) {
        Comparator<Object> comp = (Comparator<Object>) this.comparators.get(i);
        int compare = comp.compare(this.argsList.get(i).get(x.intValue()), this.argsList.get(i).get(y.intValue()));
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }

    public boolean equals(Integer x, Integer y) {
      return compare(x, y) == 0;
    }
  }
}
