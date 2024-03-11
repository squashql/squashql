package io.squashql.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MultipleColumnsSorter {

  public static int[] sort(List<List<?>> args,
                           List<Comparator<?>> comparators,
                           int[] contextIndexReaders) {
    Integer originalIndex[] = new Integer[args.get(0).size()];
    for (int i = 0; i < originalIndex.length; i++) {
      originalIndex[i] = i;
    }
    Arrays.sort(originalIndex, new IndexComparator(args, comparators, contextIndexReaders));
    return Arrays.stream(originalIndex).mapToInt(Integer::intValue).toArray();
  }

  private static class IndexComparator implements Comparator<Integer> {
    List<List<?>> argsList;
    List<Comparator<?>> comparators;
    int[] contextIndexReaders;

    public IndexComparator(List<List<?>> argsList, List<Comparator<?>> comparators, int[] contextIndexReaders) {
      this.argsList = argsList;
      this.comparators = comparators;
      this.contextIndexReaders = contextIndexReaders;
    }

    @Override
    public int compare(Integer x, Integer y) {
      for (int i = 0; i < this.argsList.size(); i++) {
        Comparator<Object> comp = (Comparator<Object>) this.comparators.get(i);
        if (comp instanceof DependentExplicitOrdering deo) {
          Object context = this.argsList.get(this.contextIndexReaders[i]).get(x);
          if (NullAndTotalComparator.isTotal(context)) {
            continue; // use the next comparator
          }
          deo.setContext(context);
          // we can use x or y independently because this comparator is used when the values in the context are equals
          // to determine in which order the current column values should be ordered.
        }
        int compare = comp.compare(this.argsList.get(i).get(x), this.argsList.get(i).get(y));
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    }
  }
}
