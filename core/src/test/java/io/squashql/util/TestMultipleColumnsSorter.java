package io.squashql.util;

import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.squashql.query.TableUtils.reorder;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

public class TestMultipleColumnsSorter {

  @Test
  void testNaturalOrder() {
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c", "a");
    List<Object> c2 = List.of(1, 2, 3, 1, 2, 4, 3, 1, 1);
    List<Object> c3 = List.of(1, 2, 3, 2, 2, 4, 3, 5, 0);

    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(naturalOrder(), naturalOrder(), naturalOrder()),
            new int[0]);
    Assertions.assertThat(sort).containsExactly(8, 0, 4, 2, 1, 6, 5, 3, 7);
  }

  @Test
  void testNaturalOrderReverse() {
    List<Object> c1 = List.of("a", "b", "a", "c", "a", "b", "b", "c", "a");
    List<Object> c2 = List.of(1, 2, 3, 1, 2, 4, 3, 1, 1);
    List<Object> c3 = List.of(1, 2, 3, 2, 2, 4, 3, 5, 0);

    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(naturalOrder().reversed(), naturalOrder().reversed(), naturalOrder().reversed()),
            new int[0]);
    Assertions.assertThat(sort).containsExactly(7, 3, 5, 6, 1, 2, 4, 0, 8);
  }

  @Test
  void testDependentExplicitOrdering() {
    List<Object> c1 = List.of("a", "a", "b", "b", "c", "c", "c");
    List<Object> c2 = List.of("x", "z", "x", "y", "x", "y", "z");
    List<Object> c3 = List.of(1, 1, 1, 1, 1, 1, 1);

    Map<Object, List<Object>> comp = new LinkedHashMap<>();
    comp.computeIfAbsent("b", k -> new ArrayList<>()).addAll(List.of("y", "x"));
    comp.computeIfAbsent("a", k -> new ArrayList<>()).addAll(List.of("z", "x"));
    comp.computeIfAbsent("c", k -> new ArrayList<>()).addAll(List.of("y", "z", "x"));

    var o1 = new CustomExplicitOrdering(List.of("b", "a", "c"));
    var o2 = DependentExplicitOrdering.create(comp);
    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2, c3),
            Arrays.asList(o1, o2, naturalOrder()),
            new int[]{-1, 0, -1});
    Assertions.assertThat(sort).containsExactly(3, 2, 1, 0, 5, 6, 4);
  }

  @Test
  void testWithNulls() {
    List<Object> c1 = Arrays.asList(null, "a", "b", "a", "b");
    List<Object> c2 = List.of(1, 2, 3, 1, 2);

    // Null is handled with special comparator
    int[] sort = MultipleColumnsSorter.sort(
            Arrays.asList(c1, c2),
            Arrays.asList(nullsLast(naturalOrder()), nullsLast(naturalOrder())),
            new int[0]);
    Assertions.assertThat(sort).containsExactly(3, 1, 4, 2, 0);
  }

  // To easily check the result.
  private void print(List<Object> c1, List<Object> c2, List<Object> c3, int[] sort) {
    List<Header> headers = Arrays.asList(
            new Header("c1", String.class, false),
            new Header("c2", String.class, false),
            new Header("c3", String.class, false));
    new ColumnarTable(headers, Collections.emptySet(), List.of(c1, c2, c3))
            .show();

    new ColumnarTable(headers, Collections.emptySet(),
            List.of(reorder(c1, sort), reorder(c2, sort), reorder(c3, sort)))
            .show();
  }

  private void print(List<Object> c1, List<Object> c2, int[] sort) {
    List<Header> headers = Arrays.asList(
            new Header("c1", String.class, false),
            new Header("c2", String.class, false));
    new ColumnarTable(headers, Collections.emptySet(), List.of(c1, c2))
            .show();

    new ColumnarTable(headers, Collections.emptySet(), List.of(reorder(c1, sort), reorder(c2, sort)))
            .show();
  }
}
