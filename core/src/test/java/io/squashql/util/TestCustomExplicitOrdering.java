package io.squashql.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

public class TestCustomExplicitOrdering {

  @Test
  void test() {
    List<String> list = new ArrayList<>(List.of("b", "d", "a", "c", "b"));
    Collections.sort(list, new CustomExplicitOrdering(List.of("b", "c")));
    // d and a not in the list should be order in natural order: a first then d
    Assertions.assertThat(list).containsExactly("b", "b", "c", "a", "d");
  }

  @Test
  void testWithTotals() {
    List<String> list = new ArrayList<>(Arrays.asList(GRAND_TOTAL, "b", TOTAL, "d", null, "a", "c", "b"));
    NullAndTotalComparator<Object> c = NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(List.of("b", "c")));
    Collections.sort(list, c);
    // d and a not in the list should be order in natural order: a first then d
    Assertions.assertThat(list).containsExactly(GRAND_TOTAL, TOTAL, "b", "b", "c", "a", "d", null);
  }
}
