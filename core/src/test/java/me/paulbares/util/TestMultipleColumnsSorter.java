package me.paulbares.util;

import me.paulbares.query.ColumnarTable;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static me.paulbares.query.TableUtils.reorder;

public class TestMultipleColumnsSorter {

  @Test
  void testNaturalOrder() {
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c", "a");
    List<Object> c2 = List.of(1, 2, 3, 1, 2, 4, 3, 1, 1);
    List<Object> c3 = List.of(1, 2, 3, 2, 2, 4, 3, 5, 0);

    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(Comparator.naturalOrder(), Comparator.naturalOrder(), Comparator.naturalOrder()));
    Assertions.assertThat(sort).containsExactly(8, 0, 4, 2, 1, 6, 5, 3, 7);
  }

  @Test
  void testNaturalOrderReverse() {
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c", "a");
    List<Object> c2 = List.of(1, 2, 3, 1, 2, 4, 3, 1, 1);
    List<Object> c3 = List.of(1, 2, 3, 2, 2, 4, 3, 5, 0);

    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(Comparator.naturalOrder().reversed(), Comparator.naturalOrder().reversed(), Comparator.naturalOrder().reversed()));
    Assertions.assertThat(sort).containsExactly(7, 3, 5, 6, 1, 2, 4, 0, 8);
  }

  // To easily check the result.
  private void print(List<Object> c1, List<Object> c2, List<Object> c3, int[] sort) {
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
}
